import json
import boto3

def lambda_handler(event, context):
    bucket_name = "logs-flipkart"
    prefix = "flagged/"
    
    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "GET"
    }

    s3_client = boto3.client("s3")
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            return {
                "statusCode": 404,
                "headers": cors_headers,
                "body": json.dumps({"message": "No files found in the bucket."})
            }

        annotated_frames_urls = []
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith("/") or "annotated_frames/" not in key:
                continue

            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': key},
                ExpiresIn=3600
            )
            annotated_frames_urls.append(url)

        return {
            "statusCode": 200,
            "headers": cors_headers,
            "body": json.dumps({"urls": annotated_frames_urls})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": cors_headers,
            "body": json.dumps({"error": str(e)})
        }