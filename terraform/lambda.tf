
data "archive_file" "lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/extract.py"
  output_path      = "${path.module}/../function.zip"
}

data "archive_file" "layer" {
  #TODO: Use this archive_file block to create a deployment package for the layer.
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../layer"
  output_path      = "${path.module}/../layer.zip"
}


resource "aws_lambda_layer_version" "dependency_layer" {
  layer_name          = "dependency_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = aws_s3_object.layer_code.key
}


resource "aws_lambda_function" "extract" {
  #TODO: Provision the lambda
  #TODO: Connect the layer which is outlined above
  filename         = "${path.module}/../function.zip"
  function_name    = var.lambda_name
  role             = aws_iam_role.lambda_role.arn
  handler          = "${path.module}/../extract.lambda_handler"

  source_code_hash = data.archive_file.lambda.output_base64sha256

  runtime          = var.python_runtime
  layers           = [aws_lambda_layer_version.dependency_layer.arn]
}