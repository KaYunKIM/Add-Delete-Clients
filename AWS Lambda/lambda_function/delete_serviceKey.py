import os
import json
import subprocess

from urllib.request import Request, urlopen


def post_msg(argStr):
    message = argStr
    
    subprocess.call(['sh', './trigger_airflow_DAG.sh', message[1]])
    
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
        # teams webhook URL
        HOOK_URL,
        data=send_text.encode('utf-8'), 
    )

    with urlopen(request) as response:
        #send teams message 
        response.read()
        

def lambda_handler(event, context):
    
    client = event['queryStringParameters']['client']
    serviceKey = event['queryStringParameters']['serviceKey']
    
    post_msg([client, serviceKey])