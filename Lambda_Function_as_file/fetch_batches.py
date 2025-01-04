import boto3
import json

def lambda_handler(event, context):
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Bucket name
    bucket_name = 'logs-flipkart'

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

    try:
        # Log the incoming event for debugging
        print("Received event:", event)

        # Parse the body to get folder_name
        if 'body' not in event or not event['body']:
            raise ValueError("Missing 'body' in the input event.")

        body = json.loads(event['body'])
        folder_name = body.get('folder_name')

        if not folder_name:
            raise ValueError("Missing 'folder_name' in the body.")

        folder_name = "unflagged/" + str(folder_name)

        # Ensure folder_name ends with a slash
        if not folder_name.endswith('/'):
            folder_name += '/'

        print("Folder name:", folder_name)

        # Fetch the list of objects in the specified prefix
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name, Delimiter='/')

        print("S3 Response:", response)

        # Extract subfolder names
        if 'CommonPrefixes' in response:
            subfolder_names = [
                item['Prefix'].replace(folder_name, '').strip('/')
                for item in response['CommonPrefixes']
            ]
        else:
            subfolder_names = []

        # Success response with CORS headers
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',  # Allow all origins
                'Access-Control-Allow-Methods': '*',
                'Access-Control-Allow-Headers': '*'
            },
            'body': json.dumps({'subfolders': subfolder_names})
        }
    except Exception as e:
        # Log the error for debugging
        print("Error:", str(e))

        # Error response with CORS headers
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
                'Access-Control-Allow-Headers': '*'
            },
            'body': json.dumps({'error': str(e)})
        }
