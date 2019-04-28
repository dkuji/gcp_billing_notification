import base64
from google.cloud import bigquery
import requests
import json
from prettytable import PrettyTable
import os

def env_vars(request):
  return os.environ.get(request, 'Specified environment variable is not set.')

def query_bq():
#  """Triggered from a message on a Cloud Pub/Sub topic.
#  Args:
#       event (dict): Event payload.
#       context (google.cloud.functions.Context): Metadata for the event.
#  """
#  pubsub_message = base64.b64decode(event['data']).decode('utf-8')
#  print(pubsub_message)
  client = bigquery.Client()
  
  query_job = client.query("""
SELECT 
  project.id as project, service.description as service, max(cost) as total 
FROM 
  `dkuji-k8s.billing.gcp_billing_export_v1_002F7A_B72135_B3C3EF` as t

GROUP BY t.project.id, service
order by 
  total desc 
LIMIT 10 ; 
      """)
  
  results = query_job.result()  # Waits for job to complete.
  return results

def post_slack(SLACK_WEBHOOK_URL):
  query_results = query_bq()

  fields_list = []
  for row in query_results:
    #table.add_row([row.project, row.service, row.total])
    tmp_list = [
      {
        "title": row.project,
        "value": row.service + ": " + str(row.total) + " USD",
        "short": "true"
      }
    ]
    fields_list.append(tmp_list[0])
    
  payload_dic = {
    "text": ":moneybag:",
    "username": "GCP bot",
    "channel": "billing",
    "attachments": [
      {
        "title": "Title",
          "text": "GCP Billing cost",
          "color": "#3AA3E3",
        "fields": []
      }
    ]
  }

  for i in fields_list:
    payload_dic['attachments'][0]['fields'].append(i)

  r = requests.post(SLACK_WEBHOOK_URL, data=json.dumps(payload_dic))

def main(event, context):
  query_results = query_bq()

  SLACK_WEBHOOK_URL = env_vars('SLACK_WEBHOOK_URL')

  post_slack(SLACK_WEBHOOK_URL)

