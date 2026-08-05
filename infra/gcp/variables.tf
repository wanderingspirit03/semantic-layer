variable "project_id" {
  description = "Dedicated staging or production GCP project ID."
  type        = string
}

variable "environment" {
  description = "Deployment environment. Use staging or prod."
  type        = string
  validation {
    condition     = contains(["staging", "prod"], var.environment)
    error_message = "environment must be staging or prod"
  }
}

variable "region" {
  type    = string
  default = "europe-west2"
  validation {
    condition     = var.region == "europe-west2"
    error_message = "pilot data residency requires europe-west2"
  }
}

variable "bucket_name" {
  description = "Globally unique private bundle bucket name."
  type        = string
}

variable "meter_bucket_name" {
  description = "Globally unique private bucket for mutable admission-meter ledgers."
  type        = string
}

variable "key_registry_secret_version" {
  description = "Explicit enabled Secret Manager registry version; changing it rolls out one coherent Cloud Run revision."
  type        = string
  validation {
    condition     = can(regex("^[1-9][0-9]*$", var.key_registry_secret_version))
    error_message = "key_registry_secret_version must be an explicit positive integer version, never latest"
  }
}

variable "container_image" {
  description = "Immutable digest-pinned ingest container image."
  type        = string
  validation {
    condition = can(regex(
      "^${var.region}-docker\\.pkg\\.dev/${var.project_id}/semantic-layer-services/semantic-layer-ingest@sha256:[0-9a-f]{64}$",
      var.container_image,
    ))
    error_message = "container_image must be this project's semantic-layer-services/semantic-layer-ingest image pinned by a 64-hex sha256 digest"
  }
}

variable "max_instances" {
  type    = number
  default = 5
}

variable "billing_account_id" {
  description = "Billing account ID used for the project budget."
  type        = string
}

variable "monthly_budget_amount" {
  description = "Monthly project budget amount used for threshold notifications."
  type        = number
  default     = 100
  validation {
    condition     = var.monthly_budget_amount > 0
    error_message = "monthly_budget_amount must be positive"
  }
}

variable "budget_currency_code" {
  description = "ISO 4217 currency code; it must match the billing account currency."
  type        = string
  default     = "USD"
  validation {
    condition     = can(regex("^[A-Z]{3}$", var.budget_currency_code))
    error_message = "budget_currency_code must be a three-letter uppercase ISO 4217 code"
  }
}

variable "ops_email" {
  description = "Email address receiving budget and ingest alerts."
  type        = string
}

variable "bundle_operator_members" {
  description = "IAM members allowed to list, fetch, validate, audit, and explicitly delete bundles."
  type        = set(string)
  default     = []
}

variable "trace_reader_members" {
  description = "IAM members allowed only to list and read completed evidence objects with the trace CLI."
  type        = set(string)
  default     = []
}

variable "release_builder_members" {
  description = "IAM members allowed to submit builds and publish digest-pinned ingest images."
  type        = set(string)
  default     = []
}

variable "key_registry_operator_members" {
  description = "IAM members allowed to add ingestion-key registry secret versions."
  type        = set(string)
  default     = []
}
