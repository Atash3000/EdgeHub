# EdgeHub Bootstrap (one-time manual setup)

Run once before the first GitHub deploy — these create the trust + secret GitHub Actions cannot create for itself.

## 1. GitHub OIDC provider + deploy role

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1

aws iam create-role --role-name edgehub-deploy \
  --assume-role-policy-document file://trust-policy.json

# Simple path for a personal project; tighten later.
aws iam attach-role-policy --role-name edgehub-deploy --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
aws iam attach-role-policy --role-name edgehub-deploy --policy-arn arn:aws:iam::aws:policy/IAMFullAccess
```

`trust-policy.json`:
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com" },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": { "token.actions.githubusercontent.com:aud": "sts.amazonaws.com" },
      "StringLike": { "token.actions.githubusercontent.com:sub": "repo:Atash3000/EdgeHub:ref:refs/heads/main" }
    }
  }]
}
```

Add the role ARN as GitHub repo **variable** `AWS_DEPLOY_ROLE_ARN` (Settings → Secrets and variables → Actions → Variables).

## 2. SSM parameters

The Finnhub key is already in SSM at `/edge-hunter/finnhub/api_key` (SecureString).

Telegram is optional — to enable it, create the bot token at `/edge-hub/telegram/api-key` AND the chat id at `/edge-hub/telegram/chat-id` (both SecureString); if either is absent the daily report is written to CloudWatch Logs instead.

```bash
aws ssm put-parameter --name /edge-hub/telegram/api-key --type SecureString --value "<BOT_TOKEN>"
aws ssm put-parameter --name /edge-hub/telegram/chat-id --type SecureString --value "<CHAT_ID>"
```

## 3. First backfill (after first successful deploy)

```bash
aws lambda invoke --function-name edgehub-daily-collector \
  --payload '{"mode":"backfill"}' --cli-binary-format raw-in-base64-out /dev/stdout
```

## 4. Verify Athena

In the Athena console (workgroup with an S3 results location set), run:
```sql
-- runId comes from metadata/current/daily_metrics/<...>.json (see Data Dictionary "Querying rule")
SELECT ticker, close, ma200, return252d FROM edgehub.daily_metrics
WHERE runId = '<runId from metadata/current>' LIMIT 20;
```
Expect rows for the accepted run.
