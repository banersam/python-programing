import logging
import sys
import smtplib
import os
import json
import boto3
import io
import csv
from email.message import EmailMessage

s3 = boto3.resource('s3')
region_dtls = 'xxxx/region_details.csv'
BUCKET_NM = os.environ['BUCKET_NAME']
SMTP_CREDENTIALS_NAME = os.environ['SMTP_CREDENTIALS_NAME']
MSG_SUBJECT = os.environ['MSG_SUBJECT']
msg_template = """\
<html>
<style type="text/css" media="screen">
    table {{
        border-collapse: collapse;
    }}
    th {{
        border: 1px solid black;
        backgorund-color: #EFF6FF;
    }}
    td {{
        border: 1px solid black;
    }}
</style>
<body>
    <p>Dear Team,</p>
	<p>{file_body}</p>
    <br>
    <pre>Please do not reply to this email, the mailbox is not monitored.
PLAI To Grow support team: Pradyuman.Mishra@sanofi.com; Samiran.Samiran-ext@sanofi.com;
</body>
</html>
"""

def regions():
    obj = s3.Object(BUCKET_NM,region_dtls).get()['Body'].read().decode('utf-8')
    reg = io.StringIO(obj)
    return 
    
    
def contacts(contact_dtls):
    obj = s3.Object(BUCKET_NM,contact_dtls).get()['Body'].read().decode('utf-8')
    f = io.StringIO(obj)
    reader = csv.DictReader(f, delimiter = ',')
    
    recipient_list=[]
    recipient_email_list=[]
    for row in reader:
        recipient_list.append(row['name'])
        recipient_email_list.append(row['email_id'])
    
    return recipient_list, recipient_email_list
    
def build_message_html(name, file_body):
    return msg_template.format(name=name, file_body=file_body)

def create_message(sender, recipient, subject, html_body):
    msg = EmailMessage()
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = subject
    msg.set_content(html_body, subtype='html')
    return msg
    
#def get_secret(secret_name, region='eu-west-1'):
def get_secret(secret_name, region=regions()):
    """
    Get secret from aws secretsmanager and ruturn parsed json

    The secret must be text (not binary)
    The secret must be json encoded

    :param secret_name: Name of secret in aws secretsmanager
    :param region: aws region of the secretsmanager
    """
    client = boto3.client('secretsmanager', region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    if 'SecretString' not in response:
        raise ValueError(f"The secret {secret_name} is not a string.")
    return json.loads(response['SecretString'])
	
def send_email_notifications(folder_nm, cedl_project_nm, file_nm, num_expected, found_file, not_found_file, not_executed_file):

   # list of recipients subscribed to job_state (success/warning/failure)
   contact_dtls = "/".join([folder_nm,file_nm])
   n, e = contacts(contact_dtls)
   email_addresses = ', '.join([str(fn) for fn in e])
   recipient_names = ', '.join([str(fn) for fn in n])
   file_names_found = '<br> '.join([str(fn) for fn in found_file])

   num_of_not_found_files = len(not_found_file)
   num_of_found_files = len(found_file)
   num_of_not_executed_files = len(not_executed_file)
   
   msg_subject = MSG_SUBJECT
   msg_subject = msg_subject.replace('folder_nm', cedl_project_nm)
   
   creds = get_secret(SMTP_CREDENTIALS_NAME)
   
   server = smtplib.SMTP(creds['host'])
   try:
    server.login(creds['username'], creds['password'])
    file_body = str(num_of_found_files) +' of '+str(num_expected) + ' expected files were processed successfully.<br><br>'
    if num_of_not_executed_files > 0:
        file_body += '<br>Files that failed proccesing:<br>'+ '<br> '.join([str(fn) for fn in not_executed_file]) 
    
    if num_of_not_found_files > 0:
        file_body += (
	       'Missing files. File(s) that match the following patterns were ' +
	       'expected, but not seen:<br>'+ '<br> '.join([str(fn) for fn in not_found_file ])
	   )
	
    body = build_message_html(recipient_names, file_body)
    msg = create_message(sender=creds['sender_address'],recipient=email_addresses, subject=msg_subject,html_body=body)
    server.send_message(msg)
   finally:
    server.quit()
   
  
