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


# ------------------------------
# Lambda IAM Policy for S3 Write
# ------------------------------

# Define

data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    effect  = "Allow"
    actions = ["s3:*"] 
  }
}

# Create
resource "aws_iam_policy" "s3_write_policy" {
  name_prefix = "s3-policy-${var.lambda_name}-write"
  description = "s3-write-for-${var.lambda_name}"


      policy = <<EOF
   {
  "Version": "2012-10-17",
  "Statement": [
    {
        "Effect": "Allow",
        "Action": [
            "logs:*"
        ],
        "Resource": "arn:aws:logs:*:*:*"
    },
    {
        "Effect": "Allow",
        "Action": [
            "s3:*",
            "s3-object-lambda:*"
        ],
        "Resource": "*"
    }
]

} 
    EOF
    }


# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_write_policy.arn
}


# ------------------------------
# Lambda IAM Policy for CloudWatch
# ------------------------------

# Define
data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {
    
    actions = [ "logs:CreateLogStream", "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${aws_lambda_function.extract.function_name}:*"
    ]
  }
}

# Create
resource "aws_iam_policy" "cw_policy" {
  name   = "${var.lambda_name}-cw-logger"
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        Action : [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Effect : "Allow",
        Resource : "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

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
resource "aws_cloudwatch_metric_alarm" "foobar" {
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
      aws_s3_bucket.data_bucket.arn,
      "${aws_s3_bucket.data_bucket.arn}/*",
      "arn:aws:s3:::*"
    ]
  }
}