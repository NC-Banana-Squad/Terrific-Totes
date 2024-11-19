
resource "aws_s3_bucket" "ingested_data_bucket" {
  #Creating s3 bucket to store our ingestion data from extract for project.
  bucket = var.ingested_data_bucket_prefix

  tags = {
    Name        = "My ingested data bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "processed_data_bucket" {
  #Creating s3 bucket to store our processed data from extract for project.
  bucket = var.processed_data_bucket_prefix

  tags = {
    Name        = "My processed data bucket"
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

resource "aws_s3_object" "extract_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "extract_lambda_function.zip"
  source = "${path.module}/../extract_function.zip"
}

  #Upload the layer code to the code_bucket.
resource "aws_s3_object" "extract_layer_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "extract_layer_code.zip"
  source = "${path.module}/../extract_layer.zip"

}


  
