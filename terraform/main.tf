terraform {
  required_providers {
    aws    = { source = "hashicorp/aws",    version = "~> 5.0" }
    random = { source = "hashicorp/random", version = "~> 3.0" }
  }
}

provider "aws" { region = var.aws_region }

resource "random_id" "suffix" { byte_length = 4 }
locals { suffix = random_id.suffix.hex }

# ── S3: quality reports + raw data ───────────────────────────────────────────

resource "aws_s3_bucket" "data" {
  bucket = "dq-platform-data-${local.suffix}"
  tags   = { Project = "data-quality-platform" }
}

resource "aws_s3_bucket" "reports" {
  bucket = "dq-platform-reports-${local.suffix}"
  tags   = { Project = "data-quality-platform" }
}

# ── SNS: quality failure alerts ──────────────────────────────────────────────

resource "aws_sns_topic" "alerts" {
  name = "dq-alerts-${local.suffix}"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ── IAM: for publishing to SNS + S3 ─────────────────────────────────────────

resource "aws_iam_role" "dq_role" {
  name = "dq-platform-role-${local.suffix}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "dq_policy" {
  name = "dq-platform-policy-${local.suffix}"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
          aws_s3_bucket.reports.arn,
          "${aws_s3_bucket.reports.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.alerts.arn
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData", "cloudwatch:GetMetricStatistics"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dq_attach" {
  role       = aws_iam_role.dq_role.name
  policy_arn = aws_iam_policy.dq_policy.arn
}

# ── CloudWatch: quality metric alarms ────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "null_rate" {
  alarm_name          = "dq-null-rate-high-${local.suffix}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "NullRate"
  namespace           = "DataQuality"
  period              = 300
  statistic           = "Average"
  threshold           = 5
  alarm_description   = "Null rate exceeded 5%"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "duplicate_rate" {
  alarm_name          = "dq-duplicate-rate-${local.suffix}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "DuplicateRate"
  namespace           = "DataQuality"
  period              = 300
  statistic           = "Average"
  threshold           = 1
  alarm_description   = "Duplicate rate exceeded 1%"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "anomaly_rate" {
  alarm_name          = "dq-anomaly-rate-${local.suffix}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "AnomalyRate"
  namespace           = "DataQuality"
  period              = 300
  statistic           = "Average"
  threshold           = 3
  alarm_description   = "Anomaly rate exceeded 3%"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "data_bucket"    { value = aws_s3_bucket.data.bucket }
output "reports_bucket" { value = aws_s3_bucket.reports.bucket }
output "sns_topic_arn"  { value = aws_sns_topic.alerts.arn }
