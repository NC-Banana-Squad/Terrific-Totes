resource "aws_s3_bucket" "data_bucket" {

  bucket = var.data_bucket_prefix

  tags = {
    Name        = "My data bucket"
    Environment = "Dev"
  }
}


resource "aws_s3_bucket" "code_bucket" {

  bucket = var.code_bucket_prefix

  tags = {
    Name        = "My code bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "function.zip"
  source = "${path.module}/../src/extract.py"
}


resource "aws_s3_object" "layer_code" {
  #TODO: Upload the layer code to the code_bucket.
  #TODO: See lambda.tf for the path to the code.
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "layer_code"
  source = "${path.module}/../layer.zip"
}