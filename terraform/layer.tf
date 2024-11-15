# #Need to use --upgrade when targeting a folder so that the install overwrites functions.
# resource "null_resource" "create_dependencies" {
#     # Install Python dependencies listed in a extract_layer_requirements.txt
#   provisioner "local-exec" {
#     command = "rm -rf ${path.module}/../extract_layer/python/* && pip install -r ${path.module}/../extract_layer_requirements.txt --upgrade -t ${path.module}/../extract_layer/python"
#   }
#   triggers = {
#     dependencies = filemd5("${path.module}/../extract_layer_requirements.txt")
#   }
# }


#   #Creates zip file after create_dependencies
# resource "null_resource" "create_extract_layer_archive" {
#   depends_on = [null_resource.create_dependencies]

#   provisioner "local-exec" {
#     command = <<EOT
#       cd ${path.module}/../extract_layer && zip -r ${path.module}/../extract_layer.zip ./*
#     EOT
#   }
#   # To make sure that extract layer archive is recreated(even when it's empty) on each run
#   triggers = {
#     archive_created = timestamp() 
#   }
# }


# Define
resource "aws_lambda_layer_version" "dependency_layer" {
  layer_name          = "dependency_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = aws_s3_object.extract_layer_code.key
  depends_on = [null_resource.create_dependencies]
  #source_code_hash = filebase64sha256("${path.module}/../extract_layer.zip")
}


#   # Deletes zip file and extract layer folder content when terraform destroy
# resource "null_resource" "cleanup" {
#   provisioner "local-exec" {
#     when    = destroy
#     command = "rm -rf ${path.module}/../extract_layer.zip ${path.module}/../extract_layer/python"
#   }
# }


