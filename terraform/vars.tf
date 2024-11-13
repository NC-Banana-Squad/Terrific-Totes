variable "lambda_name" {
  type    = string
  default = "extract"
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}

variable "ingested_data_bucket_prefix" {
  type    = string
  default = "banana-squad-ingested-data"
}

variable "processed_data_bucket_prefix" {
  type    = string
  default = "banana-squad-processed-data"
}


variable "code_bucket_prefix" {
  type    = string
  default = "banana-squad-code"
}

# variable "key" {
#   type = string
#   default = "layer_code"
# }