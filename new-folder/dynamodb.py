import boto3
import json
import io
import os
import csv
import re
import email_notification
import boto3.dynamodb.conditions as c
from datetime import date

S3 = boto3.resource('s3')
BUCKET_NM = os.environ['BUCKET_NAME']
SCAN_DT = date.today()
SCAN_DT = SCAN_DT.strftime("%Y-%m-%d")

def get_logs(cedl_project):
    items = []
    table = boto3.resource('dynamodb').Table('ProjectLog')
    prj_folder = "/{}/".format(xxx_project)
    resp = table.scan(FilterExpression= (c.Attr('source_file').contains(prj_folder) & c.Attr('execution_starttime').begins_with(SCAN_DT)))
    items.extend(resp['Items'])
    while 'LastEvaluatedKey' in resp:
        resp = table.scan(
            FilterExpression= (c.Attr('source_file').contains(prj_folder) & c.Attr('execution_starttime').begins_with(SCAN_DT)),
            ExclusiveStartKey=resp['LastEvaluatedKey']
        )
        items.extend(resp['Items'])
    return items

def lambda_handler(event, context):
    # TODO implement
    folder_nm = event['folder']
    file_nm = event['filename']
    contact_detail_file = event['filecontact']
    cedl_project = event['cedl_project_folder']
    filename = "/".join([folder_nm,file_nm])
    configobj = S3.Object(BUCKET_NM,filename).get()['Body'].read().decode('utf-8','backslashreplace')
    configstrobj = io.StringIO(configobj)
    reader = csv.DictReader(configstrobj, delimiter = ',')
    
    file_list=[]
    for row in reader:
        file_list.append(row['file_name'])
    log = get_logs(xxx_project)
    dynamo_file_list =[]
    dynamo_file_endtime = []
    
    for e in sorted(log, key=lambda a: a['execution_starttime']):
        file_name = e['source_file']
        end_time = e['execution_endtime']
        dynamo_file_list.append(file_name)
        dynamo_file_endtime.append(",".join([file_name, end_time]))
    
    not_found_file=[]
    not_executed_file =[]
    found_file=[]
    
    if len(log) > 0:           
        for f in file_list:
            r=[x for x in dynamo_file_list if re.search(f,x)]
            if len(r) == 0:
                not_found_file.append(f) 
            else:
                found_file.append(r)
                
        for file in dynamo_file_endtime:
            if file.split(',')[1] == '':
                not_executed_file.append(file.split(',')[1]) 
            
    num_expected = len(file_list)
    email_notification.send_email_notifications(folder_nm, xxx_project, contact_detail_file, num_expected, found_file, not_found_file, not_executed_file)
    
    
    
  
