package jobs

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"price-competitiveness-service/internal/providers/aurora/models"
)

func TestStaticRequestKeyIsDeterministicAndReadable(t *testing.T) {
	input := StaticPlanInput{
		ScrapePlanType: ScrapePlanStatic,
		ConfigFile:     "schedule.csv",
		RequestID:      "request-1",
	}
	definitionJSON := []byte(`{"hotel_master_id":123}`)

	first := scrapeRequestKey(input, 0, definitionJSON)
	second := scrapeRequestKey(input, 0, definitionJSON)

	if first != second {
		t.Fatalf("expected deterministic key, got %q and %q", first, second)
	}

	if !strings.HasPrefix(first, "static/schedule.csv/request-1/000001/") {
		t.Fatalf("expected readable static key prefix, got %q", first)
	}
}

func TestNeustaRequestKeyUsesPlanPrefix(t *testing.T) {
	input := StaticPlanInput{
		ScrapePlanType: ScrapePlanNeusta,
		ConfigFile:     "monday_scrape.json",
		RequestID:      "neusta-request-1",
	}
	definitionJSON := []byte(`{"hotel_master_id":123}`)

	key := scrapeRequestKey(input, 0, definitionJSON)

	if !strings.HasPrefix(key, "neusta/monday_scrape.json/neusta-request-1/000001/") {
		t.Fatalf("expected readable Neusta key prefix, got %q", key)
	}
}

func TestDynamicRequestKeyUsesPlanPrefix(t *testing.T) {
	input := StaticPlanInput{
		ScrapePlanType: ScrapePlanDynamic,
		ConfigFile:     "schedule_mon_3pm.json",
		RequestID:      "request-1",
	}
	definitionJSON := []byte(`{"hotel_master_id":123}`)

	key := scrapeRequestKey(input, 0, definitionJSON)

	if !strings.HasPrefix(key, "dynamic/schedule_mon_3pm.json/request-1/000001/") {
		t.Fatalf("expected readable dynamic key prefix, got %q", key)
	}
}

func TestStaticPlanInputKeepsProviderFlagsInDefinitions(t *testing.T) {
	input := StaticPlanInput{
		ScrapeDefinitions: []models.ScrapeDefinition{
			{ScrapeLH: true, ScrapeRTG: false},
			{ScrapeLH: false, ScrapeRTG: true},
		},
	}

	if !input.ScrapeDefinitions[0].ScrapeLH || input.ScrapeDefinitions[0].ScrapeRTG {
		t.Fatal("expected first definition to request only Aurora")
	}
	if input.ScrapeDefinitions[1].ScrapeLH || !input.ScrapeDefinitions[1].ScrapeRTG {
		t.Fatal("expected second definition to request only Rategain")
	}
}

func TestFailedPlanInputRequiresSupportedPlanAndError(t *testing.T) {
	valid := FailedPlanInput{
		TriggerType:    TriggerTypeJSON,
		ScrapePlanType: ScrapePlanDynamic,
		ConfigFile:     "schedule.json",
		RequestID:      "request-1",
		LastError:      "scrape plan has no scrape definitions",
	}

	if err := validateFailedPlan(valid); err != nil {
		t.Fatalf("expected valid failed plan input, got %v", err)
	}

	missingError := valid
	missingError.LastError = ""
	if err := validateFailedPlan(missingError); err == nil {
		t.Fatal("expected missing last error to fail validation")
	}

	unsupportedPlan := valid
	unsupportedPlan.ScrapePlanType = "unknown"
	if err := validateFailedPlan(unsupportedPlan); err == nil {
		t.Fatal("expected unsupported scrape plan type to fail validation")
	}
}

func TestFailedPlanInsertStoresTerminalJobError(t *testing.T) {
	expectedSnippets := []string{
		"INSERT INTO job_runs",
		"status",
		"finished_at",
		"last_error",
	}

	assertSQLContains(t, insertJobRunSQL, expectedSnippets)
}

func TestProviderClaimSQLClaimsOnlyRunnableRows(t *testing.T) {
	cases := map[string]struct {
		query              string
		expectedSnippets   []string
		unexpectedSnippets []string
	}{
		"aurora": {
			query: claimAuroraScrapesSQL,
			expectedSnippets: []string{
				"a.status = 'todo'",
				"a.status = 'retrying' AND (a.next_run_at IS NULL OR a.next_run_at <= now())",
				"a.status = 'running' AND a.locked_until <= now()",
				"FOR UPDATE OF a SKIP LOCKED",
				"LIMIT $1",
				"locked_by = $2",
				"locked_until = $3",
			},
			unexpectedSnippets: []string{
				"JOIN scrape_requests sr ON sr.id = a.scrape_request_id",
				"JOIN job_runs j ON j.id = sr.job_run_id",
				"j.status <> 'cancelled'",
				"ORDER BY COALESCE(a.next_run_at, a.created_at), a.created_at",
			},
		},
		"rategain": {
			query: claimRategainScrapesSQL,
			expectedSnippets: []string{
				"r.status = 'todo'",
				"r.status = 'retrying' AND (r.next_run_at IS NULL OR r.next_run_at <= now())",
				"r.status = 'submitting' AND (r.next_run_at IS NULL OR r.next_run_at <= now())",
				"JOIN job_runs j ON j.id = sr.job_run_id",
				"j.status <> 'cancelled'",
				"FOR UPDATE SKIP LOCKED",
				"LIMIT $1",
				"status = 'submitting'",
				"attempts = r.attempts + 1",
				"next_run_at = $2",
				"batch_state.submitted_at IS NOT NULL",
				"successful_batch_count",
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assertSQLContains(t, tc.query, tc.expectedSnippets)
			assertSQLExcludes(t, tc.query, tc.unexpectedSnippets)
		})
	}
}

func TestCompleteRategainSubmissionSQLMovesRowsToWaitingForCallback(t *testing.T) {
	expectedSnippets := []string{
		"INSERT INTO rategain_batches",
		"external_batch_id",
		"status = 'waiting_for_callback'",
		"submitted_at = now()",
		"WHERE scrape_request_id = ANY($1)",
	}

	joined := strings.Join([]string{
		insertRategainBatchSQL,
		completeRategainSubmissionSQL,
	}, "\n")

	assertSQLContains(t, joined, expectedSnippets)
}

func TestRetryFailedRategainScrapesSQLRequeuesFailedRows(t *testing.T) {
	expectedSnippets := []string{
		"UPDATE rategain_scrape_states rategain",
		"status = 'retrying'",
		"attempts = 0",
		"next_run_at = NULL",
		"submitted_at = NULL",
		"callback_received_at = NULL",
		"finished_at = NULL",
		"last_error = NULL",
		"JOIN job_runs job ON job.id = sr.job_run_id",
		"job.status <> 'cancelled'",
		"rategain.status = 'failed'",
		"SELECT count(*)::int FROM retried",
	}

	assertSQLContains(t, retryFailedRategainScrapesSQL, expectedSnippets)
}

func TestRategainEventSQLStoresAndProcessesCallbacks(t *testing.T) {
	expectedSnippets := map[string][]string{
		insertRategainEventSQL: {
			"INSERT INTO rategain_events",
			"external_batch_id",
			"raw_payload_json",
			"'todo'",
		},
		claimRategainEventsSQL: {
			"WHERE status = 'todo'",
			"FOR UPDATE SKIP LOCKED",
		},
		listRategainBatchItemsForEventSQL: {
			"JOIN rategain_batch_items",
			"batches.external_batch_id = $1",
			"array_agg(items.scrape_request_id::text ORDER BY items.request_item_id)",
			"array_agg(items.request_item_id ORDER BY items.request_item_id)",
			"GROUP BY batches.id",
		},
		upsertRategainResponseSQL: {
			"provider",
			"'rategain'",
			"ON CONFLICT (scrape_request_id, provider) DO UPDATE",
			"report_status = 'done'",
			"reported_at = now()",
		},
		completeRategainCallbackSQL: {
			"status = 'done'",
			"callback_received_at = now()",
			"WHERE scrape_request_id = ANY($1)",
		},
		completeRategainEventSQL: {
			"status = 'processed'",
			"processed_at = now()",
		},
		failRategainEventSQL: {
			"status = 'failed'",
			"last_error = $2",
		},
	}

	for sql, snippets := range expectedSnippets {
		assertSQLContains(t, sql, snippets)
	}
}

func TestMatchRategainCallbackResponsesUsesRequestFieldsNotCallbackItemID(t *testing.T) {
	requestPayload := []byte(`{
		"id": "request-1:0",
		"item": [
			{"id":1,"hid":989507,"cin":"20260806","los":7,"oc":2,"ch":0,"cur":"GBP","wid":"TUICOUK"},
			{"id":2,"hid":989508,"cin":"20260807","los":7,"oc":2,"ch":0,"cur":"GBP","wid":"JET2"}
		]
	}`)
	eventPayload := []byte(`{
		"ReportID": "request-1:0",
		"BID": "1",
		"APIKey": "hotel-only-key",
		"item": [
			{"itemid":999,"CIN":"2026-08-07","LOS":7,"OC":2,"OCC":0,"CR":"GBP","WID":"JET2","Hotel":{"HID":"989508"}},
			{"itemid":998,"CIN":"2026-08-06","LOS":7,"OC":2,"OCC":0,"CR":"GBP","WID":"TUICOUK","Hotel":{"RHID":"989507"}}
		]
	}`)

	responses, err := matchRategainCallbackResponses(eventPayload, []rategainBatchEventRow{
		{
			RequestPayloadJSON: requestPayload,
			Items: []rategainBatchEventItem{
				{
					ScrapeRequestID: "scrape-request-1",
					RequestItemID:   1,
				},
				{
					ScrapeRequestID: "scrape-request-2",
					RequestItemID:   2,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("expected callback to match request fields, got %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("expected two scrape responses, got %#v", responses)
	}

	itemsByScrapeRequest := decodeRategainResponseItems(t, responses)
	if got := itemsByScrapeRequest["scrape-request-1"][0]["itemid"]; got != float64(998) {
		t.Fatalf("expected scrape-request-1 to match item 998, got %v", got)
	}
	if got := itemsByScrapeRequest["scrape-request-2"][0]["itemid"]; got != float64(999) {
		t.Fatalf("expected scrape-request-2 to match item 999, got %v", got)
	}
}

func decodeRategainResponseItems(
	t *testing.T,
	responses []RategainCallbackResponse,
) map[string][]map[string]any {
	t.Helper()

	decoded := make(map[string][]map[string]any)
	for _, response := range responses {
		var payload struct {
			Items []map[string]any `json:"item"`
		}
		if err := json.Unmarshal(response.ResponseJSON, &payload); err != nil {
			t.Fatalf("failed to decode response JSON: %v", err)
		}
		decoded[response.ScrapeRequestID] = payload.Items
	}

	return decoded
}

func assertSQLContains(t *testing.T, query string, expectedSnippets []string) {
	t.Helper()
	for _, expected := range expectedSnippets {
		if !strings.Contains(query, expected) {
			t.Fatalf("expected SQL to contain %q", expected)
		}
	}
}

func assertSQLExcludes(t *testing.T, query string, unexpectedSnippets []string) {
	t.Helper()
	for _, unexpected := range unexpectedSnippets {
		if strings.Contains(query, unexpected) {
			t.Fatalf("expected SQL to exclude %q", unexpected)
		}
	}
}

func TestAuroraResponseUpsertIsIdempotent(t *testing.T) {
	expectedSnippets := []string{
		"ON CONFLICT (scrape_request_id, provider) DO UPDATE",
		"response_json = EXCLUDED.response_json",
		"report_status = 'todo'",
		"report_attempts = 0",
	}

	for _, expected := range expectedSnippets {
		if !strings.Contains(upsertAuroraResponseSQL, expected) {
			t.Fatalf("expected response upsert SQL to contain %q", expected)
		}
	}
}

func TestListScrapeRequestsKeepsAuroraDebugLazy(t *testing.T) {
	expectedSnippets := []string{
		"sr.scrape_definition_json->>'hotel_master_id' AS hotel_master_id",
		"sr.scrape_definition_json#>>'{stay_duration_config,checkin_date}' AS checkin_date",
		"($2 = '' OR aurora.status = $2)",
		"($3 = '' OR rategain.status = $3)",
		"ORDER BY sr.id ASC",
		"LIMIT $4",
		"OFFSET $5",
	}

	assertSQLContains(t, listScrapeRequestsSQL, expectedSnippets)

	lazySnippets := []string{
		"aurora_response.response_json::text AS aurora_response_json",
		"aurora.attempts AS aurora_attempts",
		"aurora.last_error AS aurora_last_error",
		"ORDER BY sr.created_at",
	}
	assertSQLExcludes(t, listScrapeRequestsSQL, lazySnippets)
}

func TestGetAuroraDebugIncludesResponse(t *testing.T) {
	expectedSnippets := []string{
		"JOIN aurora_scrape_states aurora",
		"LEFT JOIN scrape_responses aurora_response",
		"aurora_response.provider = 'aurora'",
		"sr.job_run_id = $1",
		"sr.id = $2",
		"aurora_response.response_json::text AS aurora_response_json",
	}

	assertSQLContains(t, getAuroraDebugSQL, expectedSnippets)
}

func TestGetRategainDebugIncludesBatchesAndResponse(t *testing.T) {
	expectedSnippets := []string{
		"WITH request_batches AS",
		"JOIN rategain_batches batches",
		"JOIN rategain_scrape_states rategain",
		"LEFT JOIN scrape_responses rategain_response",
		"rategain_response.provider = 'rategain'",
		"LEFT JOIN request_batches",
		"sr.job_run_id = $1",
		"sr.id = $2",
		"rategain_response.response_json::text AS rategain_response_json",
		"request_batches.batches_json",
	}

	assertSQLContains(t, getRategainDebugSQL, expectedSnippets)
}

func TestScrapeRequestStatusQueriesSupportProgressAndFilters(t *testing.T) {
	expectedSnippets := map[string][]string{
		listAuroraStatusCountsSQL: {
			"JOIN aurora_scrape_states",
			"GROUP BY aurora.status",
		},
		listRategainStatusCountsSQL: {
			"JOIN rategain_scrape_states",
			"GROUP BY rategain.status",
		},
		countScrapeRequestsSQL: {
			"count(*)::int",
			"($2 = '' OR aurora.status = $2)",
			"($3 = '' OR rategain.status = $3)",
		},
	}

	for sql, snippets := range expectedSnippets {
		for _, expected := range snippets {
			if !strings.Contains(sql, expected) {
				t.Fatalf("expected scrape request query to contain %q", expected)
			}
		}
	}
}

func TestListJobsQueryMatchesJobsPageSummaryQuery(t *testing.T) {
	expectedQuery := `
select
    j.id,
    j.trigger_type,
    j.scrape_plan_type,
    j.status,
    j.config_file,
    j.request_id,
    j.created_at,
    j.started_at,
    j.finished_at,
    j.last_error,
    count(s.id) as scrape_req,
    count(a.scrape_request_id) as a_scrape_req,
    count(r.scrape_request_id) as r_scrape_req,
    count(a.scrape_request_id) filter (where a.status='done') as a_scrape_req_done,
    count(r.scrape_request_id) filter (where r.status='done') as r_scrape_req_done
from job_runs j join scrape_requests s on s.job_run_id=j.id
left join aurora_scrape_states a on s.id=a.scrape_request_id
left join rategain_scrape_states r on s.id=r.scrape_request_id
group by j.id, j.created_at
order by j.created_at desc
limit 20`

	if strings.TrimSpace(listJobsSQL) != strings.TrimSpace(expectedQuery) {
		t.Fatalf("list jobs query does not match expected jobs page summary query")
	}
}

func TestGetJobQueryIncludesCoverageAndTimelinessMetrics(t *testing.T) {
	expectedSnippets := []string{
		"successful_scrape_count",
		"coverage_percent",
		"first_success_seconds",
		"p50_success_seconds",
		"p99_success_seconds",
		"last_success_seconds",
		"avg_success_seconds",
		"percentile_cont(0.50)",
		"percentile_cont(0.99)",
		"status = 'done'",
	}

	for _, expected := range expectedSnippets {
		if !strings.Contains(getJobSQL, expected) {
			t.Fatalf("expected job query to contain %q", expected)
		}
	}
}

func TestClaimReportsSQLClaimsRunnableReportsWithLease(t *testing.T) {
	expectedSnippets := []string{
		"r.provider = 'aurora'",
		"r.report_status = 'todo'",
		"r.report_status = 'retrying' AND (r.report_next_run_at IS NULL OR r.report_next_run_at <= now())",
		"FOR UPDATE OF r SKIP LOCKED",
		"report_status = 'retrying'",
		"report_attempts = r.report_attempts + 1",
		"report_next_run_at = $2",
	}
	assertSQLContains(t, claimReportsSQL, expectedSnippets)

	unexpectedSnippets := []string{
		"JOIN job_runs j ON j.id = sr.job_run_id",
		"j.status <> 'cancelled'",
		"ORDER BY COALESCE(r.report_next_run_at, r.created_at), r.created_at",
	}
	assertSQLExcludes(t, claimReportsSQL, unexpectedSnippets)
}

func TestReportCompletionAndFailureSQLUpdatesStatus(t *testing.T) {
	expectedSnippets := map[string][]string{
		completeReportSQL: {
			"report_status = 'done'",
			"reported_at = now()",
			"report_destination_uri = $2",
		},
		retryReportSQL: {
			"report_status = 'retrying'",
			"report_next_run_at = $2",
			"report_last_error = $3",
		},
		failReportSQL: {
			"report_status = 'failed'",
			"report_next_run_at = NULL",
			"report_last_error = $2",
		},
	}

	for sql, snippets := range expectedSnippets {
		for _, expected := range snippets {
			if !strings.Contains(sql, expected) {
				t.Fatalf("expected report SQL to contain %q", expected)
			}
		}
	}
}

func TestRefreshJobStatusSQLAggregatesProviderStates(t *testing.T) {
	expectedSnippets := []string{
		"JOIN aurora_scrape_states",
		"JOIN rategain_scrape_states",
		"WHEN failed_count > 0 THEN 'failed'",
		"WHEN running_count > 0 THEN 'running'",
		"WHEN total_count > 0 AND pending_count = 0 THEN 'succeeded'",
		"ELSE 'planned'",
		"j.status <> 'cancelled'",
	}

	for _, expected := range expectedSnippets {
		if !strings.Contains(refreshJobStatusSQL, expected) {
			t.Fatalf("expected refresh SQL to contain %q", expected)
		}
	}
}

func TestMarkJobRunningSQLOnlyTransitionsPlannableJobs(t *testing.T) {
	expected := "status IN ('planning', 'planned')"
	if !strings.Contains(markJobRunningSQL, expected) {
		t.Fatalf("expected mark running SQL to contain %q", expected)
	}

	notExpected := "status IN ('planning', 'planned', 'running')"
	if strings.Contains(markJobRunningSQL, notExpected) {
		t.Fatalf("expected mark running SQL not to rewrite already-running jobs")
	}
}

func TestRetryNextRunAtUsesBoundedLinearDelay(t *testing.T) {
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)

	if got := retryNextRunAt(now, 1); !got.Equal(now.Add(time.Minute)) {
		t.Fatalf("expected first retry after 1 minute, got %s", got)
	}

	if got := retryNextRunAt(now, 45); !got.Equal(now.Add(30 * time.Minute)) {
		t.Fatalf("expected retry delay capped at 30 minutes, got %s", got)
	}
}

func TestErrorTextHandlesNilErrors(t *testing.T) {
	if got := errorText(nil); got != "" {
		t.Fatalf("expected empty text for nil error, got %q", got)
	}
	if got := errorText(errors.New("boom")); got != "boom" {
		t.Fatalf("expected error text, got %q", got)
	}
}
