# Create load lambda role
resource "aws_iam_role" "load_lambda_role" {
  name_prefix        = "role-load-lambda"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

#Create load lambda policy
resource "aws_iam_policy" "load_lambda_policy" {
  name        = "load-lambda-policy"
  description = "IAM policy for Lambda to access S3 buckets and CloudWatch logs"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["s3:GetObject"],
        Effect = "Allow",
        Resource = "arn:aws:s3:::banana-squad-code/*"
      },
      {
        Action = ["s3:GetObject"],
        Effect = "Allow",
        Resource = "arn:aws:s3:::banana-squad-processed-data/*"
      },
      {
        Action = ["s3:ListBucket"],
        Effect = "Allow",
        Resource = "arn:aws:s3:::banana-squad-processed-data"
      },
      {
        Action = [
          "rds-db:connect"
          ],
        Effect = "Allow",
        Resource = "arn:aws:rds:eu-west-2:418295700587:db:postgres"
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Effect = "Allow",
        Resource = "arn:aws:logs:eu-west-2:418295700587:log-group:/aws/lambda/load:*"
      },
      {
        Action = ["sns:Publish"],
        Effect = "Allow",
        Resource = "arn:aws:sns:eu-west-2:418295700587:alert-sre"
      },
      {
        Action = ["secretsmanager:GetSecretValue"],
        Effect = "Allow",
        Resource = "arn:aws:secretsmanager:eu-west-2:${data.aws_caller_identity.current.account_id}:secret:datawarehouse_credentials*"
      }
    ]
  })
}

#Attach policy to the role
resource "aws_iam_role_policy_attachment" "load_lambda_policy_attach" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.load_lambda_policy.arn
}
