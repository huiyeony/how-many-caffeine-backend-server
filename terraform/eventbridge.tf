# EventBridge가 Lambda를 호출할 수 있는 IAM Role
resource "aws_iam_role" "eventbridge_invoke" {
  name = "crawl-pipeline-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_invoke_policy" {
  name = "crawl-pipeline-eventbridge-policy"
  role = aws_iam_role.eventbridge_invoke.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action   = "lambda:InvokeFunction"
      Effect   = "Allow"
      Resource = aws_lambda_function.crawl_pipeline.arn
    }]
  })
}

# EventBridge 스케줄 (매주 월요일 새벽 3시 KST = 일요일 UTC 18:00)
resource "aws_scheduler_schedule" "crawl_weekly" {
  name                         = "howmanycaffeine-crawl-weekly"
  schedule_expression          = "cron(0 18 ? * SUN *)"
  schedule_expression_timezone = "Asia/Seoul"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.crawl_pipeline.arn
    role_arn = aws_iam_role.eventbridge_invoke.arn
  }
}

# Lambda에 EventBridge 호출 허용
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.crawl_pipeline.function_name
  principal     = "scheduler.amazonaws.com"
  source_arn    = aws_scheduler_schedule.crawl_weekly.arn
}
