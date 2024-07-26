terraform {
  required_providers {
    bitbucket = {
      source  = "DrFaust92/bitbucket"
      version = "2.32.0"
    }
  }
}

resource "bitbucket_project" "data-engineering-basic" {
  owner      = var.bitbucket_workspace # must be a team
  name       = "data-engineering-basic-2"
  key        = "DEB"
  is_private = true
}

resource "bitbucket_repository" "data-engineering-basic" {
  owner      = var.bitbucket_workspace
  name       = "data-engineering-basic-2"
  scm        = "git"
  is_private = true
  project_key = bitbucket_project.data-engineering-basic.key
  pipelines_enabled = true
}

resource "bitbucket_pipeline_ssh_key" "repo_ssh_keypem" {
  workspace   = var.bitbucket_workspace
  repository  = bitbucket_repository.data-engineering-basic.id
  public_key  = file("${path.module}/bitbucket_secret/id_rsa.pub")
  private_key = file("${path.module}/bitbucket_secret/id_rsa")
}