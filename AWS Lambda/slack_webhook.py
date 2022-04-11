import os
import json
import pymongo
import datetime

from urllib.request import Request, urlopen

def post_msg(argStr):
    # message = [siteNm, serviceKey, conStartDt, conEndDt, seviceEndDt]
    message = argStr
    
    send_data = {
      "@type": "MessageCard",
      "@context": "http://schema.org/extensions",
      "themeColor": "0076D7",
      "summary": "Larry Bryant created a new task",
      "sections": [{
          "activityTitle": message[0],
          "activitySubtitle": message[1],
          "facts": [{
              "name": "계약기간",
              "value": "{} ~ {}".format(message[3], message[4])
          }, {
              "name": "서비스종료일",
              "value": message[5],
          }, {
              "name": "Status",
              "value": message[2],
          }],
      }],
      "potentialAction": [{
          "@type": "ActionCard",
          "name": "Feedback",
          "inputs": [{
              "@type": "TextInput",
              "id": "comment",
              "title": "해당 고객사에 대한 추가 의견을 달아주세요"
          }],
          "actions": [{
              "@type": "HttpPOST",
              "name": "Add comment",
              "target": "http://:8080/admin/rest_api/api?api=trigger_dag",
          }]
      }, {
        "@type": "HttpPOST",
        "name": "Learn More",
        "target": "http://:8080/admin/rest_api/api?api=list_dags",
        "body": "serviceKey={{message[1]}}"
      }]
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
    
    # connect to MongoDB
    db_con = os.environ['URI']
    connection = pymongo.MongoClient(db_con)
    
    # connect to MongoDB Database
    database = connection.get_database('Gr')
    
    today = datetime.datetime.today().strftime("%Y-%m-%d")
    last_month = datetime.datetime.today() - datetime.timedelta(days=50)
    last_month = last_month.strftime("%Y-%m-%d")
    
    data_cursor = database.get_collection('Site').find({"useCd":"US", "conEndDt":{"$gte" : last_month,  "$lte" : today} })
    
    for data in data_cursor:
        siteNm = data['siteNm']
        serviceKey = data['serviceKey']
        useCd = data['useCd']
        conStartDt = data['conStartDt']
        conEndDt = data['conEndDt']
        seviceEndDt = data['seviceEndDt']
        
        post_msg([siteNm, serviceKey, useCd, conStartDt, conEndDt, seviceEndDt])