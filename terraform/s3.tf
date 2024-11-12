resource "aws_s3_bucket" "data_bucket" {
  #Creating s3 bucket to store our ingestion data from extract for project.
  bucket = var.data_bucket_prefix

  tags = {
    Name        = "My data bucket"
    Environment = "Dev"
  }
}


resource "aws_s3_bucket" "code_bucket" {
  #Creating s3 bucket to store our code for project.
  bucket = var.code_bucket_prefix

  tags = {
    Name        = "My code bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_object" "extra_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "extract_lambda_function.zip"
  source = "${path.module}/../src/extract.py"
}


resource "aws_s3_object" "extract_layer_code" {
  #Upload the layer code to the code_bucket.
  #See lambda.tf for the path to the code.
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "extract_layer_code"
  source = "${path.module}/../extract_layer.zip"
}