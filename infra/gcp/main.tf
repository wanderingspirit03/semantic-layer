locals {
  service_name = "semantic-layer-ingest-${var.environment}"
  labels = {
    service     = "semantic-layer-ingest"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "google_project_service" "required" {
  for_each = toset([
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "billingbudgets.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "secretmanager.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
  ])
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

data "google_project" "current" {
  project_id = var.project_id
}

resource "google_artifact_registry_repository" "ingest" {
  project       = var.project_id
  location      = var.region
  repository_id = "semantic-layer-services"
  description   = "Digest-pinned Semantic Layer service images"
  format        = "DOCKER"
  labels        = local.labels
  depends_on    = [google_project_service.required]
}

resource "google_storage_bucket" "bundles" {
  name                        = var.bucket_name
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = local.labels
  depends_on                  = [google_project_service.required]

  versioning { enabled = false }
  soft_delete_policy { retention_duration_seconds = 0 }
}

resource "google_storage_bucket" "metering" {
  name                        = var.meter_bucket_name
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = local.labels
  depends_on                  = [google_project_service.required]

  versioning { enabled = false }
  soft_delete_policy { retention_duration_seconds = 0 }
}

resource "google_storage_bucket" "build_source" {
  name                        = "${var.project_id}-sl-build-source-${var.environment}"
  project                     = var.project_id
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = local.labels
  depends_on                  = [google_project_service.required]

  versioning { enabled = false }
  soft_delete_policy { retention_duration_seconds = 0 }
}

resource "google_secret_manager_secret" "key_registry" {
  secret_id = "semantic-layer-ingest-key-registry-${var.environment}"
  project   = var.project_id
  labels    = local.labels
  replication {
    user_managed {
      replicas { location = var.region }
    }
  }
  depends_on = [google_project_service.required]
}

resource "google_service_account" "ingest" {
  project      = var.project_id
  account_id   = "sl-ingest-${var.environment}"
  display_name = "Semantic Layer ingest (${var.environment})"
}

resource "google_service_account" "build" {
  project      = var.project_id
  account_id   = "sl-build-${var.environment}"
  display_name = "Semantic Layer image builder (${var.environment})"
}

resource "google_project_iam_custom_role" "ingest_objects" {
  project     = var.project_id
  role_id     = "semanticLayerIngestObjects${title(var.environment)}"
  title       = "Semantic Layer ingest object writer (${var.environment})"
  description = "Create and read immutable ingest objects; cannot overwrite or delete them."
  permissions = ["storage.objects.create", "storage.objects.get"]
}

resource "google_project_iam_custom_role" "operator_objects" {
  project     = var.project_id
  role_id     = "semanticLayerBundleOperator${title(var.environment)}"
  title       = "Semantic Layer bundle operator (${var.environment})"
  description = "List, fetch, and validate evidence objects; cannot create, overwrite, or delete them."
  permissions = ["storage.objects.get", "storage.objects.list"]
}

resource "google_project_iam_custom_role" "operator_delete" {
  project     = var.project_id
  role_id     = "semanticLayerBundleDelete${title(var.environment)}"
  title       = "Semantic Layer evidence-prefix deleter (${var.environment})"
  description = "Trusted operators may delete approved bundle/upload objects; exact command scope is procedural and audited."
  permissions = ["storage.objects.delete"]
}

resource "google_project_iam_custom_role" "operator_audit" {
  project     = var.project_id
  role_id     = "semanticLayerBundleAudit${title(var.environment)}"
  title       = "Semantic Layer deletion audit writer (${var.environment})"
  description = "Create append-only deletion audit objects only through an audit-prefix binding."
  permissions = ["storage.objects.create"]
}

resource "google_project_iam_custom_role" "ingest_metering" {
  project     = var.project_id
  role_id     = "semanticLayerIngestMetering${title(var.environment)}"
  title       = "Semantic Layer ingest metering (${var.environment})"
  description = "Create, read, and conditionally replace isolated admission ledgers."
  permissions = ["storage.objects.create", "storage.objects.delete", "storage.objects.get", "storage.objects.update"]
}

resource "google_project_iam_audit_config" "storage_data_write" {
  project = var.project_id
  service = "storage.googleapis.com"
  audit_log_config {
    log_type = "DATA_WRITE"
  }
  audit_log_config {
    log_type = "DATA_READ"
  }
}

resource "google_storage_bucket_iam_member" "ingest_objects" {
  bucket = google_storage_bucket.bundles.name
  role   = google_project_iam_custom_role.ingest_objects.name
  member = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_storage_bucket_iam_member" "ingest_metering" {
  bucket = google_storage_bucket.metering.name
  role   = google_project_iam_custom_role.ingest_metering.name
  member = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_secret_manager_secret_iam_member" "ingest_registry" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.key_registry.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.ingest.email}"
}

resource "google_storage_bucket_iam_member" "operators" {
  for_each = var.bundle_operator_members
  bucket   = google_storage_bucket.bundles.name
  role     = google_project_iam_custom_role.operator_objects.name
  member   = each.value
}

resource "google_storage_managed_folder" "completed_evidence" {
  bucket = google_storage_bucket.bundles.name
  name   = "tenants/"
}

resource "google_storage_managed_folder_iam_member" "trace_readers" {
  for_each       = var.trace_reader_members
  bucket         = google_storage_bucket.bundles.name
  managed_folder = google_storage_managed_folder.completed_evidence.name
  role           = google_project_iam_custom_role.operator_objects.name
  member         = each.value
}

resource "google_storage_bucket_iam_member" "operator_delete" {
  for_each = var.bundle_operator_members
  bucket   = google_storage_bucket.bundles.name
  role     = google_project_iam_custom_role.operator_delete.name
  member   = each.value
  condition {
    title       = "evidence_and_upload_prefixes_only"
    description = "Trusted operators may delete evidence/upload objects but never append-only audit objects."
    expression  = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.bundles.name}/objects/tenants/') || resource.name.startsWith('projects/_/buckets/${google_storage_bucket.bundles.name}/objects/uploads/')"
  }
}

resource "google_storage_bucket_iam_member" "operator_audit" {
  for_each = var.bundle_operator_members
  bucket   = google_storage_bucket.bundles.name
  role     = google_project_iam_custom_role.operator_audit.name
  member   = each.value
  condition {
    title       = "append_only_deletion_audit_prefix"
    description = "Audit creation cannot create or replace evidence."
    expression  = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.bundles.name}/objects/audit/')"
  }
}

resource "google_storage_bucket_iam_member" "meter_operators" {
  for_each = var.bundle_operator_members
  bucket   = google_storage_bucket.metering.name
  role     = google_project_iam_custom_role.ingest_metering.name
  member   = each.value
}

resource "google_artifact_registry_repository_iam_member" "operators" {
  for_each   = var.release_builder_members
  project    = var.project_id
  location   = google_artifact_registry_repository.ingest.location
  repository = google_artifact_registry_repository.ingest.name
  role       = "roles/artifactregistry.writer"
  member     = each.value
}

resource "google_artifact_registry_repository_iam_member" "build" {
  project    = var.project_id
  location   = google_artifact_registry_repository.ingest.location
  repository = google_artifact_registry_repository.ingest.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.build.email}"
}

resource "google_project_iam_member" "build_logs" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.build.email}"
}

resource "google_storage_bucket_iam_member" "build_source" {
  bucket = google_storage_bucket.build_source.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.build.email}"
}

resource "google_storage_bucket_iam_member" "operator_build_source" {
  for_each = var.release_builder_members
  bucket   = google_storage_bucket.build_source.name
  role     = "roles/storage.objectCreator"
  member   = each.value
}

resource "google_project_iam_member" "operator_builds" {
  for_each = var.release_builder_members
  project  = var.project_id
  role     = "roles/cloudbuild.builds.editor"
  member   = each.value
}

resource "google_service_account_iam_member" "operator_build_identity" {
  for_each           = var.release_builder_members
  service_account_id = google_service_account.build.name
  role               = "roles/iam.serviceAccountUser"
  member             = each.value
}

resource "google_secret_manager_secret_iam_member" "operator_versions" {
  for_each  = var.key_registry_operator_members
  project   = var.project_id
  secret_id = google_secret_manager_secret.key_registry.secret_id
  role      = "roles/secretmanager.secretVersionAdder"
  member    = each.value
}

resource "google_cloud_run_v2_service" "ingest" {
  name                = local.service_name
  project             = var.project_id
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = var.environment == "prod"
  labels              = local.labels
  scaling {
    scaling_mode       = "AUTOMATIC"
    min_instance_count = 0
  }
  depends_on = [
    google_project_service.required,
    google_storage_bucket_iam_member.ingest_objects,
    google_storage_bucket_iam_member.ingest_metering,
    google_secret_manager_secret_iam_member.ingest_registry,
  ]

  template {
    service_account                  = google_service_account.ingest.email
    timeout                          = "900s"
    max_instance_request_concurrency = 1
    scaling {
      min_instance_count = 0
      max_instance_count = var.max_instances
    }
    containers {
      image = var.container_image
      resources {
        limits   = { cpu = "1", memory = "4Gi" }
        cpu_idle = true
      }
      env {
        name  = "SEMANTIC_LAYER_BUCKET"
        value = google_storage_bucket.bundles.name
      }
      env {
        name  = "SEMANTIC_LAYER_METER_BUCKET"
        value = google_storage_bucket.metering.name
      }
      env {
        name = "SEMANTIC_LAYER_KEY_REGISTRY_JSON"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.key_registry.secret_id
            version = var.key_registry_secret_version
          }
        }
      }
      startup_probe {
        initial_delay_seconds = 0
        timeout_seconds       = 2
        period_seconds        = 5
        failure_threshold     = 12
        http_get { path = "/health" }
      }
      liveness_probe {
        timeout_seconds   = 2
        period_seconds    = 30
        failure_threshold = 3
        http_get { path = "/health" }
      }
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "public_ingest" {
  project  = var.project_id
  location = google_cloud_run_v2_service.ingest.location
  name     = google_cloud_run_v2_service.ingest.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_monitoring_notification_channel" "ops_email" {
  project      = var.project_id
  display_name = "Semantic Layer ${var.environment} operations"
  type         = "email"
  labels = {
    email_address = var.ops_email
  }
  depends_on = [google_project_service.required]
}

resource "google_logging_metric" "ingest_5xx" {
  project = var.project_id
  name    = "semantic_layer_ingest_${var.environment}_5xx"
  filter  = "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${local.service_name}\" AND jsonPayload.status>=500"
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_logging_metric" "ingest_auth" {
  project = var.project_id
  name    = "semantic_layer_ingest_${var.environment}_auth_failures"
  filter  = "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${local.service_name}\" AND jsonPayload.status=401"
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_logging_metric" "ingest_validation" {
  project = var.project_id
  name    = "semantic_layer_ingest_${var.environment}_validation_conflicts"
  filter  = "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${local.service_name}\" AND (jsonPayload.status=400 OR jsonPayload.status=409)"
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_logging_metric" "ingest_capacity" {
  project = var.project_id
  name    = "semantic_layer_ingest_${var.environment}_capacity_rejections"
  filter  = "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${local.service_name}\" AND jsonPayload.status=429"
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_logging_metric" "ingest_meter_pressure" {
  project = var.project_id
  name    = "semantic_layer_ingest_${var.environment}_meter_pressure"
  filter  = "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${local.service_name}\" AND (jsonPayload.meter_pressure=\"warning\" OR jsonPayload.meter_pressure=\"critical\")"
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

locals {
  ingest_alerts = {
    five_xx = {
      display_name = "Semantic Layer ingest 5xx (${var.environment})"
      metric_name  = google_logging_metric.ingest_5xx.name
      window       = "300s"
      threshold    = 0
    }
    persistent_auth = {
      display_name = "Semantic Layer persistent ingest auth failures (${var.environment})"
      metric_name  = google_logging_metric.ingest_auth.name
      window       = "900s"
      threshold    = 9
    }
    validation_conflicts = {
      display_name = "Semantic Layer ingest validation or digest conflicts (${var.environment})"
      metric_name  = google_logging_metric.ingest_validation.name
      window       = "300s"
      threshold    = 0
    }
    capacity_rejections = {
      display_name = "Semantic Layer ingest capacity rejections (${var.environment})"
      metric_name  = google_logging_metric.ingest_capacity.name
      window       = "300s"
      threshold    = 0
    }
    meter_pressure = {
      display_name = "Semantic Layer ingest meter pressure (${var.environment})"
      metric_name  = google_logging_metric.ingest_meter_pressure.name
      window       = "300s"
      threshold    = 0
    }
  }
}

resource "google_monitoring_alert_policy" "ingest" {
  for_each              = local.ingest_alerts
  project               = var.project_id
  display_name          = each.value.display_name
  combiner              = "OR"
  notification_channels = [google_monitoring_notification_channel.ops_email.name]

  conditions {
    display_name = each.value.display_name
    condition_threshold {
      filter          = "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/${each.value.metric_name}\""
      comparison      = "COMPARISON_GT"
      threshold_value = each.value.threshold
      duration        = each.value.window
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_SUM"
        cross_series_reducer = "REDUCE_SUM"
      }
      trigger { count = 1 }
    }
  }
}

resource "google_billing_budget" "project" {
  billing_account = var.billing_account_id
  display_name    = "Semantic Layer ${var.environment} monthly budget"
  budget_filter {
    projects = ["projects/${data.google_project.current.number}"]
  }
  amount {
    specified_amount {
      currency_code = var.budget_currency_code
      units         = tostring(var.monthly_budget_amount)
    }
  }
  threshold_rules { threshold_percent = 0.5 }
  threshold_rules { threshold_percent = 0.9 }
  threshold_rules { threshold_percent = 1.0 }
  all_updates_rule {
    monitoring_notification_channels = [google_monitoring_notification_channel.ops_email.name]
    disable_default_iam_recipients   = false
  }
}
