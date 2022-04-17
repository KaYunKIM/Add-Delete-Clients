import os
import json
import pymongo
import datetime
import subprocess
import paramiko
import boto3

from urllib.request import Request, urlopen


def post_msg(argStr):
    message = argStr
    
    s3_client = boto3.client('s3')
    s3_client.download_file('s3groobeeds', 'groobeeds.pem', '/tmp/groobeeds.pem')
    
    send_data = {
      "@type": "MessageCard",
      "@context": "http://schema.org/extensions",
      "themeColor": "0076D7",
      "summary": "{} 해지완료".format(message[0]),
      "sections": [{
          "activityTitle": message[0],
          "activitySubtitle": "해지완료",
      }],
    }
    
    HOOK_URL = os.environ['HookUrl']
    send_text = json.dumps(send_data)

    request = Request(
        # teams URL
        HOOK_URL,
        data=send_text.encode('utf-8'),
    )

    with urlopen(request) as response:
        #teams_message
        response.read()


def lambda_handler(event, context):
    
    # connect to MongoDB
    db_con = os.environ['URI']
    connection = pymongo.MongoClient(db_con)
    
    # connect to MongoDB Database
    database = connection.get_database('Grb')
    
    serviceKey = event['queryStringParameters']['serviceKey']
    data_cursor = database.get_collection('Site').find({"serviceKey":serviceKey})
    client_name = data_cursor[0]['siteNm']

    post_msg([client_name, serviceKey])