locals {
  data_lake_bucket = "data_lake"
}

variable "project" {
  default = "de-zoomcamp-375618"
}

variable "region" {
  default = "europe-west6"
  type    = string
}

variable "zone" {
  default = "europe-west6-a"
  type    = string
}

variable "storage_class" {
  default = "STANDARD"
}

variable "BQ_DATASET" {
  default = "iowa_liquor"
  type    = string
}