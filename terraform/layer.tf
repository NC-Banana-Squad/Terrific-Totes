# Define
resource "aws_lambda_layer_version" "dependency_layer" {
  layer_name          = "dependency_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = aws_s3_object.extract_layer.key
}

output "layer_arn_with_version" {
  value = aws_lambda_layer_version.dependency_layer.arn
}