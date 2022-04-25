import os
import json
import pymongo
import boto3
import time

from urllib.request import Request, urlopen


def post_msg(argStr):
    message = argStr
    
    ssm_client = boto3.client('ssm')
    response = ssm_client.send_command(
            # airflow-main-server 
            InstanceIds=['i-0eddf'],
            DocumentName="AWS-RunShellScript",
            Parameters={
                'commands': [
                    "export HOME=/home/ec2-user",
                    "/home/ec2-user/trigger_delete_servicKey_DAG.sh {}".format(message[1]),
                ]
            },
        )
    time.sleep(3)

    output = ssm_client.get_command_invocation(
            CommandId=response['Command']['CommandId'],
            InstanceId='i-0eddf',
        )
    
    print(output)
    
    send_data = {
      "@type": "MessageCard",
      "@context": "http://schema.org/extensions",
      "themeColor": "0076D7",
      "summary": "{} 해지완료".format(message[0]),
      "sections": [{
          "activityTitle": message[1],
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