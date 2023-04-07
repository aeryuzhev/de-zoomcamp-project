terraform {
  required_version = ">= 1.0"
  backend "local" {}

  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "data-lake-bucket" {
  name                        = "${var.project}_${local.data_lake_bucket}"
  location                    = var.region
  storage_class               = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age = 30
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}

# resource "google_compute_instance" "vm_instance" {
#   name         = "iowa-instance"
#   machine_type = "e2-standard-2"
#   zone         = var.zone

#   boot_disk {
#     initialize_params {
#       image = "ubuntu-os-cloud/ubuntu-1804-lts"
#       size  = "20"
#     }
#   }

#   network_interface {
#     network = "default"
#     access_config {
#     }
#   }

#   service_account {
#     email  = "iowa-service-account@de-zoomcamp-375618.iam.gserviceaccount.com"
#     scopes = ["cloud-platform"]
#   }
# }
