import boto3
import requests
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError

region = 'us-east-1' # e.g. us-east-1
SENDER = 'timkea@amazon.com'
SUBJECT = '***Important Maintenance Issue***'
BODY = """
    <h1>Hello Apple Holler Teammate!</h1><p>Thank you for your dedication to keeping Apple Holler beautiful.</p>
    There is an issue that requires your attention: https://d2kfl22iskrweh.cloudfront.net/{thumbnail}
    Please click on this link to see the location of the issue: {maps_url}
        """
CHARSET = 'UTF-8'
client = boto3.client('ses',region_name=region)
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'https://search-serverless-docrepo-fi4e6ior6zma7qm4pj7xule6ju.us-east-1.es.amazonaws.com' # the Amazon ES domain, with https://
index = 'lambda-index'
type = '_doc'
url = host + '/' + index + '/' + type + '/'

headers = { "Content-Type": "application/json" }

def lambda_handler(event, context):
    count = 0
    for record in event['Records']:
        # Get the primary key for use as the Elasticsearch ID
        id = record['dynamodb']['Keys']['id']['S']

        if record['eventName'] == 'REMOVE':
            r = requests.delete(url + id, auth=awsauth)
        else:
            document = record['dynamodb']['NewImage']
            r = requests.put(url + id, auth=awsauth, json=document, headers=headers)
        count += 1
        # Custom code for email notification
        processing_status = record['dynamodb']['NewImage']['ProcessingStatus']['S']
        if processing_status != "SUCCEEDED":
            continue
        thumbnail = record['dynamodb']['NewImage']['thumbnail']['M']['key']['S']
        thumbnail = thumbnail.split('/')[-1]
        print(thumbnail)
        lat_deg = int(record['dynamodb']['NewImage']['geoLocation']['M']['Latitude']['M']['D']['N'])
        lat_min = int(record['dynamodb']['NewImage']['geoLocation']['M']['Latitude']['M']['M']['N'])
        lat_sec = float(record['dynamodb']['NewImage']['geoLocation']['M']['Latitude']['M']['S']['N'])
        lat_dir = record['dynamodb']['NewImage']['geoLocation']['M']['Latitude']['M']['Direction']['S']
        lon_deg = int(record['dynamodb']['NewImage']['geoLocation']['M']['Longtitude']['M']['D']['N'])
        lon_min = int(record['dynamodb']['NewImage']['geoLocation']['M']['Longtitude']['M']['M']['N'])
        lon_sec = float(record['dynamodb']['NewImage']['geoLocation']['M']['Longtitude']['M']['S']['N'])
        lon_dir = record['dynamodb']['NewImage']['geoLocation']['M']['Longtitude']['M']['Direction']['S']
        latitude_deg = lat_deg + (lat_min / 60) + (lat_sec / 3600)
        longitude_deg = -1 * (lon_deg + (lon_min / 60) + (lon_sec / 3600))
        maps_url = "https://maps.google.com/maps?q=" + str(latitude_deg) + "," + str(longitude_deg)
        print(maps_url) 
        object_detected = record['dynamodb']['NewImage']['objectDetected']['L']
        objects = []
        for obj in object_detected:
            objects.append(obj["S"])
        print(objects)
        obj_detected = False
        if 'Trash Can' in objects:
            obj_detected = True
            send_email(thumbnail, "timkea+sanitation@amazon.com", maps_url)
        if 'Tree' in objects:
            obj_detected = True
            send_email(thumbnail, "timkea+groundscrew@amazon.com", maps_url)
        if 'Animal' in objects:
            obj_detected = True
            send_email(thumbnail, "timkea+animalcontrol@amazon.com", maps_url)
        if not obj_detected:
            send_email(thumbnail, "timkea+appleholleradmin@amazon.com", maps_url)
            
    return str(count) + ' records processed.'
    
    
def send_email(image_name, recipient, maps_url):
    try: 
        response = client.send_email(
        Destination={
            'ToAddresses': [
                recipient,
                "timkea+appleholleradmin@amazon.com"
            ],
        },
        Message={
            'Body': {
                'Html': {
                    'Charset': CHARSET,
                    'Data': BODY.format(thumbnail=image_name, maps_url=maps_url),
                },
                'Text': {
                    'Charset': CHARSET,
                    'Data': BODY.format(thumbnail=image_name, maps_url=maps_url),
                },
            },
            'Subject': {
                'Charset': CHARSET,
                'Data': SUBJECT,
            },
        },
        Source=SENDER,
        # If you are not using a configuration set, comment or delete the
        # following line
        #ConfigurationSetName=CONFIGURATION_SET,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
    