
data "archive_file" "lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/extract.py"
  output_path      = "${path.module}/../function.zip"
}


resource "aws_lambda_function" "extract" {
  #TODO: Provision the lambda
  #TODO: Connect the layer which is outlined above
  filename         = "${path.module}/../function.zip"
  function_name    = var.lambda_name
  role             = aws_iam_role.lambda_role.arn
  handler          = "extract.lambda_handler"

  source_code_hash = data.archive_file.lambda.output_base64sha256

  runtime          = var.python_runtime
  layers           = [aws_lambda_layer_version.dependency_layer.arn]
}