#Need to use --upgrade when targeting a folder so that the install overwrites functions.
resource "null_resource" "create_dependencies" {
  provisioner "local-exec" {
    command = <<EOT
      tmp_dir="${GITHUB_WORKSPACE}/temp_extract_layer"
      rm -rf $tmp_dir/*
      pip install -r ${path.module}/../extract_layer_requirements.txt --upgrade -t $tmp_dir
    EOT
  }
  triggers = {
    dependencies = filemd5("${path.module}/../extract_layer_requirements.txt")
  }
}


  #Creates zip file after create_dependencies
resource "null_resource" "create_extract_layer_archive" {
  depends_on = [null_resource.create_dependencies]

  provisioner "local-exec" {
   command = <<EOT
      layer_dir="${GITHUB_WORKSPACE}/extract_layer"
      mkdir -p $layer_dir  # Ensure directory exists
      # Ensure the install worked by checking files are in the directory
      if [ "$(ls -A $layer_dir)" ]; then
        cd $layer_dir && zip -r ${GITHUB_WORKSPACE}/extract_layer.zip ./*
      else
        echo "No files to zip, skipping..."
      fi
    EOT
  }
  # To make sure that extract layer archive is recreated(even when it's empty) on each run
  triggers = {
    archive_created = timestamp() 
  }
}

# Define
resource "aws_lambda_layer_version" "dependency_layer" {
  layer_name          = "dependency_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = aws_s3_object.extract_layer_code.key
  depends_on = [null_resource.create_dependencies]
  #source_code_hash = filebase64sha256("${path.module}/../extract_layer.zip")
}


  # Deletes zip file and extract layer folder content when terraform destroy
resource "null_resource" "cleanup" {
  provisioner "local-exec" {
    when    = destroy
    command = "rm -rf ${path.module}/../extract_layer.zip ${path.module}/../extract_layer/python"
  }
}



# data "null_data_source" "wait_for_lambda_exporter" {
#   inputs = {
#     # This ensures that this data resource will not be evaluated until
#     # after the null_resource has been created.
#     lambda_exporter_id = "${null_resource.create_dependencies.id}"

#     # This value gives us something to implicitly depend on
#     # in the archive_file below.
#     source_dir = "${path.module}/../extract_layer/python"
#   }
# }

# This is needed because data blocks do not use 'depend on' so empty archive_file will throw error.
/* resource "null_resource" "archive_trigger" {
  depends_on = [null_resource.create_dependencies]
}

data "archive_file" "extract_layer" {
  # Creates a deployment package for the layer.
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../extract_layer"
  output_path      = "${path.module}/../extract_layer.zip"
  depends_on = [null_resource.archive_trigger]
} */
