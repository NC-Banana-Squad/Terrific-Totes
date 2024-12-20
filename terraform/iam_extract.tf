
# Define standard trust policy for Lambdas
data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Create extract lambda role
resource "aws_iam_role" "extract_lambda_role" {
  name_prefix        = "role-${var.lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

#Create extract lambda policy
resource "aws_iam_policy" "extract_lambda_policy" {
  name        = "extract-lambda-policy"
  description = "IAM policy for Lambda to access S3 buckets and CloudWatch logs"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["s3:GetObject", "s3:PutObject"],
        Effect = "Allow",
        Resource = "arn:aws:s3:::banana-squad-code/*"
      },
      {
        Action = ["s3:ListBucket"],
        Effect = "Allow",
        Resource = "arn:aws:s3:::banana-squad-code"
      },
      {
        Action = ["s3:PutObject"],
        Effect = "Allow",
        Resource = "arn:aws:s3:::banana-squad-ingested-data/*"
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        
        Effect = "Allow",
        Resource = "arn:aws:logs:eu-west-2:418295700587:log-group:/aws/lambda/extract:*"
      },
      {
        Action = ["secretsmanager:GetSecretValue"],
        Effect = "Allow",
        Resource = "arn:aws:secretsmanager:eu-west-2:${data.aws_caller_identity.current.account_id}:secret:database_credentials*"
      },
      {
        Action = ["sns:Publish"],
        Effect = "Allow",
        Resource = "arn:aws:sns:eu-west-2:418295700587:extract-errors-topic"
      }
    ]
  })
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_policy_attachment" {
    role = aws_iam_role.extract_lambda_role.name
    policy_arn = aws_iam_policy.extract_lambda_policy.arn
    lifecycle {
    create_before_destroy = true
  }
}
