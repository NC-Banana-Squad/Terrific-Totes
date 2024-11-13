
resource "null_resource" "create_dependencies" {
    # Install Python dependencies listed in a extract_layer_requirements.txt
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../extract_layer_requirements.txt -t ${path.module}/../extract_layer/python"
  }
  triggers = {
    dependencies = filemd5("${path.module}/../extract_layer_requirements.txt")
  }
}

data "archive_file" "extract_layer" {
  #Creates a deployment package for the layer.
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../extract_layer"
  output_path      = "${path.module}/../extract_layer.zip"
}



resource "aws_lambda_layer_version" "dependency_layer" {
  layer_name          = "dependency_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = aws_s3_object.extract_layer_code.key
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
}



