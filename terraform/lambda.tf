#creates an archive file for extract lambda function. So lambda can use extract.py file.
/* data "archive_file" "extract_lambda" {
  type             = "zip"
  output_file_mode = "0666"

  source {
    content  = file("${path.module}/../src/extract.py")
    filename = "extract.py"
  }

  source {
    content  = file("${path.module}/../src/util_functions.py")
    filename = "extract_util_fuctions.py"
  }

  output_path      = "${path.module}/../extract_function.zip"
} */

resource "null_resource" "extract_lambda" {
  
  provisioner "local-exec" {
    command = "rm -f ${path.module}/../src/extract/extract_lambda.zip && zip -j ${path.module}/../src/extract/extract_lambda.zip ${path.module}/../src/extract/extract.py ${path.module}/../src/extract/util_functions.py"
  }

  triggers = {
    dependencies = "${filemd5("${path.module}/../src/extract/extract.py")}-${filemd5("${path.module}/../src/extract/util_functions.py")}"

  }
}

#May want to use the source code hash correctly with an archive layer. 

resource "aws_lambda_function" "extract" {
  #Creates a Lambda function called extract with dependency layer.
  #Connect the layer which is outlined above
  filename         = "${path.module}/../src/extract/extract_lambda.zip"
  function_name    = var.lambda_name
  role             = aws_iam_role.lambda_role.arn
  handler          = "extract.lambda_handler"
  depends_on = [null_resource.extract_lambda]
  #source_code_hash = filebase64sha256("${path.module}/../src/extract/extract_lambda.zip")
  runtime          = var.python_runtime
  layers           = [aws_lambda_layer_version.dependency_layer.arn]
  timeout          = 20
}

