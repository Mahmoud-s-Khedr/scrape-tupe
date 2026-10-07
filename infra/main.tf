locals {
  name = "scrape-tupe"
  services = toset([
    "run.googleapis.com",
    "firestore.googleapis.com",
    "storage.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudscheduler.googleapis.com",
    "billingbudgets.googleapis.com",
    "monitoring.googleapis.com",
    "secretmanager.googleapis.com",
  ])
}
data "google_project" "current" { project_id = var.project_id }
locals { master_uri = "https://${local.name}-master-${data.google_project.current.number}.${var.region}.run.app" }

resource "google_project_service" "api" {
  for_each           = local.services
  service            = each.value
  disable_on_destroy = false
}
resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = local.name
  format        = "DOCKER"
}
resource "google_firestore_database" "default" {
  name        = "(default)"
  location_id = var.region
  type        = "FIRESTORE_NATIVE"
}
# Queue claims use bounded ordered queries; declaring these indexes prevents a
# production queue from falling back to an accidental collection scan.
resource "google_firestore_index" "sources_discovery_claim" {
  collection = "sources"
  fields {
    field_path = "state"
    order      = "ASCENDING"
  }
  fields {
    field_path = "kind"
    order      = "ASCENDING"
  }
  fields {
    field_path = "created_at"
    order      = "ASCENDING"
  }
}
resource "google_firestore_index" "raw_urls_claim" {
  collection = "raw_urls"
  fields {
    field_path = "state"
    order      = "ASCENDING"
  }
  fields {
    field_path = "created_at"
    order      = "ASCENDING"
  }
}
resource "google_firestore_index" "videos_download_claim" {
  collection = "videos"
  fields {
    field_path = "state"
    order      = "ASCENDING"
  }
  fields {
    field_path = "created_at"
    order      = "ASCENDING"
  }
}
resource "google_firestore_index" "executions_guard" {
  collection = "executions"
  fields {
    field_path = "kind"
    order      = "ASCENDING"
  }
  fields {
    field_path = "state"
    order      = "ASCENDING"
  }
}
resource "google_firestore_index" "videos_lease_recovery" {
  collection = "videos"
  fields {
    field_path = "state"
    order      = "ASCENDING"
  }
  fields {
    field_path = "lease_until"
    order      = "ASCENDING"
  }
}
resource "google_firestore_index" "raw_urls_lease_recovery" {
  collection = "raw_urls"
  fields {
    field_path = "state"
    order      = "ASCENDING"
  }
  fields {
    field_path = "lease_until"
    order      = "ASCENDING"
  }
}
resource "google_firestore_index" "sources_lease_recovery" {
  collection = "sources"
  fields {
    field_path = "state"
    order      = "ASCENDING"
  }
  fields {
    field_path = "lease_until"
    order      = "ASCENDING"
  }
}
resource "google_storage_bucket" "artifacts" {
  name                        = "${var.project_id}-scrape-tupe-artifacts"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  lifecycle_rule {
    action { type = "Delete" }
    condition {
      age            = 1
      matches_prefix = ["videos/"]
    }
  }
}
resource "google_service_account" "master" {
  account_id = "scrape-tupe-master"
}
resource "google_service_account" "normalizer" {
  account_id = "scrape-tupe-normalizer"
}
resource "google_service_account" "downloader" {
  account_id = "scrape-tupe-downloader"
}
resource "google_service_account" "scheduler" { account_id = "scrape-tupe-scheduler" }
resource "google_project_iam_member" "master_firestore" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.master.email}"
}
resource "google_project_iam_member" "master_run_jobs" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.master.email}"
}
resource "google_secret_manager_secret" "operator_api_key" {
  secret_id = "scrape-tupe-operator-api-key"
  replication {
    auto {}
  }
}
resource "google_secret_manager_secret_version" "operator_api_key" {
  secret      = google_secret_manager_secret.operator_api_key.id
  secret_data = var.operator_api_key
}
resource "google_secret_manager_secret_iam_member" "master_operator_key" {
  secret_id = google_secret_manager_secret.operator_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.master.email}"
}
resource "google_project_iam_custom_role" "downloader_artifacts" {
  role_id     = "scrapeTupeArtifactWriter"
  title       = "Scrape-Tupe artifact writer"
  permissions = ["storage.objects.create", "storage.objects.get"]
}
resource "google_storage_bucket_iam_member" "downloader_objects" {
  bucket = google_storage_bucket.artifacts.name
  role   = google_project_iam_custom_role.downloader_artifacts.name
  member = "serviceAccount:${google_service_account.downloader.email}"
  condition {
    title      = "videos_only"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.artifacts.name}/objects/videos/')"
  }
}
resource "google_cloud_run_v2_service" "master" {
  name     = "${local.name}-master"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"
  template {
    service_account = google_service_account.master.email
    scaling { min_instance_count = 0 }
    containers {
      image = var.master_image
      env {
        name  = "GCS_BUCKET"
        value = google_storage_bucket.artifacts.name
      }
      env {
        name  = "MASTER_STORE"
        value = "firestore"
      }
      env {
        name  = "MASTER_AUDIENCE"
        value = local.master_uri
      }
      env {
        name  = "WORKER_SERVICE_ACCOUNTS"
        value = "${google_service_account.normalizer.email},${google_service_account.downloader.email}"
      }
      env {
        name  = "DISCOVERY_JOB"
        value = "${local.name}-discovery"
      }
      env {
        name  = "NORMALIZER_JOB"
        value = "${local.name}-normalizer"
      }
      env {
        name  = "DOWNLOADER_JOB"
        value = "${local.name}-downloader"
      }
      env {
        name = "OPERATOR_API_KEY"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.operator_api_key.secret_id
            version = "latest"
          }
        }
      }
      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = var.project_id
      }
      env {
        name  = "GCP_REGION"
        value = var.region
      }
    }
  }
}
resource "google_cloud_run_v2_job" "normalizer" {
  name     = "${local.name}-normalizer"
  location = var.region
  template {
    parallelism = var.normalizer_parallelism
    task_count  = var.normalizer_parallelism
    template {
      service_account = google_service_account.normalizer.email
      timeout         = "900s"
      max_retries     = 0
      containers {
        image = var.normalizer_image
        env {
          name  = "MASTER_URL"
          value = google_cloud_run_v2_service.master.uri
        }
        env {
          name  = "MASTER_AUDIENCE"
          value = google_cloud_run_v2_service.master.uri
        }
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }
        env {
          name  = "NORMALIZER_MODE"
          value = "url"
        }
      }
    }
  }
}
resource "google_cloud_run_v2_job" "discovery" {
  name     = "${local.name}-discovery"
  location = var.region
  template {
    template {
      service_account = google_service_account.normalizer.email
      timeout         = "900s"
      max_retries     = 0
      containers {
        image = var.normalizer_image
        env {
          name  = "MASTER_URL"
          value = google_cloud_run_v2_service.master.uri
        }
        env {
          name  = "MASTER_AUDIENCE"
          value = google_cloud_run_v2_service.master.uri
        }
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }
        env {
          name  = "NORMALIZER_MODE"
          value = "discovery"
        }
      }
    }
  }
}
resource "google_cloud_run_v2_service_iam_member" "normalizer_master_invoker" {
  name     = google_cloud_run_v2_service.master.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.normalizer.email}"
}
resource "google_cloud_run_v2_service_iam_member" "downloader_master_invoker" {
  name     = google_cloud_run_v2_service.master.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.downloader.email}"
}
resource "google_cloud_run_v2_service_iam_member" "scheduler_master_invoker" {
  name     = google_cloud_run_v2_service.master.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}
# Submission is authenticated by the shared operator key in the application;
# worker-only routes additionally validate an ID token in the master.
resource "google_cloud_run_v2_service_iam_member" "operators_master_invoker" {
  for_each = var.operator_members
  name     = google_cloud_run_v2_service.master.name
  location = var.region
  role     = "roles/run.invoker"
  member   = each.value
}
resource "google_cloud_scheduler_job" "reconcile" {
  name     = "${local.name}-reconcile"
  region   = var.region
  schedule = "* * * * *"
  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.master.uri}/internal/reconcile"
    headers     = { "X-API-Key" = var.operator_api_key }
    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloud_run_v2_service.master.uri
    }
  }
}
resource "google_cloud_run_v2_job" "downloader" {
  name     = "${local.name}-downloader"
  location = var.region
  template {
    task_count  = 10
    parallelism = 10
    template {
      service_account = google_service_account.downloader.email
      timeout         = "2700s"
      containers {
        image = var.downloader_image
        env {
          name  = "MASTER_URL"
          value = google_cloud_run_v2_service.master.uri
        }
        env {
          name  = "MASTER_AUDIENCE"
          value = google_cloud_run_v2_service.master.uri
        }
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.artifacts.name
        }
        env {
          name  = "STORAGE_BACKEND"
          value = "gcs"
        }
        env {
          name  = "EXIT_WHEN_EMPTY"
          value = "true"
        }
      }
    }
  }
}

resource "google_project_iam_custom_role" "master_artifacts" {
  role_id     = "scrapeTupeArtifactInspector"
  title       = "Scrape-Tupe artifact metadata reader"
  permissions = ["storage.objects.get"]
}
resource "google_storage_bucket_iam_member" "master_artifacts" {
  bucket = google_storage_bucket.artifacts.name
  role   = google_project_iam_custom_role.master_artifacts.name
  member = "serviceAccount:${google_service_account.master.email}"
  condition {
    title      = "videos_only"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.artifacts.name}/objects/videos/')"
  }
}
