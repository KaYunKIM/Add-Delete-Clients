import os
import json
import pymongo
import datetime

from urllib.request import Request, urlopen


def post_msg(argStr):
    message = argStr
    
    send_data = {
      "@type": "MessageCard",
      "@context": "http://schema.org/extensions",
      "themeColor": "0076D7",
      "summary": message[0],
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
          "name": "Comment",
          "inputs": [{
              "@type": "TextInput",
              "id": "comment",
              "title": "해당 고객사에 대한 추가 의견을 달아주세요"
          }],
          "actions": [{
              "@type": "HttpPOST",
              "name": "Comment",
              "target": "https://docs.microsoft.com/outlook/actionable-messages"
          },{
              "@type": "HttpPOST",
              "name": "Delete",
              # API Gateway HTTP POST API => trigger delete_serviceKey lambda funtion
              "target": "https://pkr6te3hh0.execute-api.ap-northeast-2.amazonaws.com/dev/deleteServiceKey?serviceKey={}".format(message[1])
          }]
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
    database = connection.get_database('Grb')
    
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