# ---------------
# Lambda IAM Role
# ---------------

# Define
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

# Create
resource "aws_iam_role" "lambda_role" {
  name_prefix        = "role-${var.lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

resource "aws_iam_policy" "lambda_policy" {
  name        = "lambda-s3-logs-policy"
  description = "IAM policy for Lambda to access S3 buckets and CloudWatch logs"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["s3:GetObject", "s3:ListBucket", "s3:ListObject", "s3:PutObject"],
        Effect = "Allow",
        Resource = "arn:aws:s3:::banana-squad-code"
      },
      # {
      #   Action = ["s3:ListBucket"],
      #   Effect = "Allow",
      #   Resource = "arn:aws:s3:::banana-squad-code/*"
      # },
      # {
      #   Action = ["s3:ListObject"],
      #   Effect = "Allow",
      #   Resource = "arn:aws:s3:::banana-squad-code/*"
      # },
      {
        Action = ["s3:PutObject"],
        Effect = "Allow",
        Resource = "arn:aws:s3:::ingested_data_bucket"
      },
      # {
      #   Action = ["s3:PutObject"],
      #   Effect = "Allow",
      #   Resource = "arn:aws:s3:::banana-squad-code/*"
      # },
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
        Resource = "arn:aws:secretsmanager:::database_credentials"
      }
    ]
  })
}



# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.lambda_policy.arn
    lifecycle {
    create_before_destroy = true
  }
}

# resource "aws_iam_role_policy_attachment" "s3_bucket_data_policy" {
#     role = aws_iam_policy.s3_write_policy.name
#     policy_arn = data.aws_iam_policy_document.s3_data_policy_doc.arn
# }

# ------------------------------
# Lambda IAM Policy for CloudWatch
# ------------------------------

# # Define
# data "aws_iam_policy_document" "cw_document" {
#   statement {

#     actions = [ "logs:CreateLogGroup" ]

#     resources = [
#       "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
#     ]
#   }

#   statement {
    
#     actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

#     resources = [
#       "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.extract.function_name}:*"
#     ]
#   }
# }

# # Create
# resource "aws_iam_policy" "cw_policy" {
#   name   = "${var.lambda_name}-cw-logger"
#   policy = jsonencode({
#     "Version" : "2012-10-17",
#     "Statement" : [
#       {
#         Action : [
#           "logs:CreateLogStream",
#           "logs:PutLogEvents"
#         ],
#         Effect : "Allow",
#         Resource : "arn:aws:logs:*:*:*"
#       }
#     ]
#   })
# }

# # Attach
# resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
#   role       = aws_iam_role.lambda_role.name
#   policy_arn = aws_iam_policy.cw_policy.arn
#   lifecycle {
#     create_before_destroy = true
#   }
# }

resource "aws_cloudwatch_log_group" "log_group" {
  name              = "/aws/lambda/${aws_lambda_function.extract.function_name}"
  retention_in_days = 7
  lifecycle {
    prevent_destroy = false
  }
}


#IAM policy for the SNS emails. Adds the policy to the role. Allows start and get queries

resource "aws_iam_role_policy" "sns_policy" {
  name = "sns-publish"
  role = aws_iam_role.lambda_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "sns:Publish",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "logs:StartQuery",
          "logs:GetQueryResults",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "iam:ListAccountAliases",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })
}

#Creates an SNS alert topic.
resource "aws_sns_topic" "alert_sre" {
  name = "alert-sre"
}

#Creates an SNS subscription to allow emails to be sent to given email address
resource "aws_sns_topic_subscription" "sre_email_subscription" {
  topic_arn = aws_sns_topic.alert_sre.arn
  protocol  = "email"
  endpoint  = "ciarankyle@gmail.com"
}


#Created metric filter for "ERROR" in cw logs.
resource "aws_cloudwatch_log_metric_filter" "error_filter" {
  name           = "MyAppAccessCount"
  pattern        = "\"ERROR\""
  log_group_name = aws_cloudwatch_log_group.log_group.name

  metric_transformation {
    name      = "EventCount"
    namespace = "applicationErrors"
    value     = "1"
  }
}

#Uses metric_filter to create cloudwatch alarm. Runs once every 2 mins for now.
resource "aws_cloudwatch_metric_alarm" "metric_alarm" {
  alarm_name                = "extract_email_notifications"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = aws_cloudwatch_log_metric_filter.error_filter.metric_transformation[0].name
  namespace                 = aws_cloudwatch_log_metric_filter.error_filter.metric_transformation[0].namespace
  period                    = 120
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "This metric monitors ec2 cpu utilization"

  alarm_actions = [
    aws_sns_topic.alert_sre.arn
  ]
}

#IAM policy allows for listing all buckets and putting objects.
data "aws_iam_policy_document" "s3_document" {
  statement {

    actions = ["s3:PutObject", "s3:ListAllMyBuckets"]

    resources = [
      aws_s3_bucket.ingested_data_bucket.arn,
      "${aws_s3_bucket.ingested_data_bucket.arn}/*",
      "arn:aws:s3:::*"
    ]
  }
}