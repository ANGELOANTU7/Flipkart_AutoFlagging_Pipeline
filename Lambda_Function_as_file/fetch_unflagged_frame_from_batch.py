import json
import boto3

def lambda_handler(event, context):
    if event['httpMethod'] == 'OPTIONS':
        # Return CORS headers for preflight requests
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
                'Access-Control-Allow-Headers': '*'
            },
            'body': ''
        }

    bucket_name = "logs-flipkart"  # Replace with your S3 bucket name

    # Parse the body for input parameters
    try:
        body = json.loads(event.get("body", "{}"))
        device_name = body.get("device_name")
        batch_number = body.get("batch_number")
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
                'Access-Control-Allow-Headers': '*'
            },
            "body": json.dumps({"message": "Invalid JSON in request body."})
        }

    if not device_name or not batch_number:
        return {
            "statusCode": 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
                'Access-Control-Allow-Headers': '*'
            },
            "body": json.dumps({"message": "device_name and batch_number are required."})
        }

    prefix = f"unflagged/{device_name}/{batch_number}/annotated_frames/"
    s3_client = boto3.client("s3")

    try:
        # Fetch all objects under the specified prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' not in response:
            return {
                "statusCode": 404,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': '*',
                    'Access-Control-Allow-Headers': '*'
                },
                "body": json.dumps({"message": "No files found for the specified device and batch."})
            }

        annotated_frames_urls = []

        # Loop through all objects and collect their URLs
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith("/"):
                continue  # Skip folders

            # Generate a presigned URL for the object (valid for 1 hour)
            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': key},
                ExpiresIn=3600
            )
            annotated_frames_urls.append(url)

        return {
            "statusCode": 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
                'Access-Control-Allow-Headers': '*'
            },
            "body": json.dumps({"urls": annotated_frames_urls})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
                'Access-Control-Allow-Headers': '*'
            },
            "body": json.dumps({"error": str(e)})
        }
