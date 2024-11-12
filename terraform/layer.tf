resource "null_resource" "create_dependencies" {
    # Install Python dependencies listed in a requirements.txt
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../extract_layer_requirements.txt -t ${path.module}/../layer/python"
  }
  triggers = {
    dependencies = filemd5("${path.module}/../extract_layer_requirements.txt")
  }
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