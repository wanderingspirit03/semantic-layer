output "ingest_url" { value = google_cloud_run_v2_service.ingest.uri }
output "bucket_name" { value = google_storage_bucket.bundles.name }
output "meter_bucket_name" { value = google_storage_bucket.metering.name }
output "build_source_bucket_name" { value = google_storage_bucket.build_source.name }
output "key_registry_secret" { value = google_secret_manager_secret.key_registry.secret_id }
output "service_account" { value = google_service_account.ingest.email }
output "artifact_registry_repository" { value = google_artifact_registry_repository.ingest.name }
output "build_service_account" { value = google_service_account.build.email }
