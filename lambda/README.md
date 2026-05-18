# Lambda handler

Self-contained `handler.py` that reverses an MSISDN hash by fetching a 2 MB
range of `hashes.bin` from S3 per call. `boto3` is in the Lambda runtime, so
no dependencies need to be bundled.

## Deploy (zip, manual)

```bash
cd lambda
zip msisdn-lookup.zip handler.py
aws lambda create-function \
  --function-name msisdn-lookup \
  --runtime python3.13 \
  --role arn:aws:iam::<account>:role/<lambda-role> \
  --handler handler.handler \
  --zip-file fileb://msisdn-lookup.zip \
  --environment "Variables={LOOKUP_BUCKET=my-bucket,LOOKUP_KEY=hashes.bin,LOOKUP_RECORD_COUNT=200000000}" \
  --timeout 10 --memory-size 256
```

The execution role needs `s3:GetObject` on the bucket holding `hashes.bin`.

## Invoke

```bash
aws lambda invoke --function-name msisdn-lookup \
  --payload '{"hash":"172509f6416f41d1ce3b78a757c1d4ce90fc1ab1c9d4cdf1edf25ab7bf3fbdfd"}' \
  --cli-binary-format raw-in-base64-out out.json
cat out.json
# {"statusCode": 200, "body": "{\"phone\": \"254700000001\"}"}
```

## Cost

One `GetObject` per invocation. At $0.0004 / 1000 requests, 100k calls is
about $0.04. Storage for the 2.8 GB file is roughly $0.07/month (Standard
us-east-1).
