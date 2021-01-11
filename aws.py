"""
Created on Sat Aug 18 14:35:56 2018

"""
import os
import sys
import datetime
import time
import cx_Oracle
import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
#import botocore

os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"

class logger:
    ### initLog
    ### This function initialize the log file, the file path is defined as <root folder>/logs, the file name is defined in settings file
    ### this function is called only once
    ### Log file name format: <File name>_<Date(YYYYMMDD)>_<Time(HH24MISS>.log
    def __init__(self, loggingFolder = '', logFileName = '', file = False, stream = False, logMode = 0, logStream = '', custom = False, logsClient = None, cwLogGroup = '', retentionDays = 0):
        self.logFileName = logFileName
        self.logFile = None
        self.file = file
        self.logStream = logStream
        self.stream = stream
        self.logMode = logMode
        self.kinesisClient = None
        self.custom = custom
        self.logsClient = logsClient
        self.cwLogGroup = cwLogGroup
        self.cwLogStream = ''
        self.retentionDays = retentionDays
        
        if self.custom:
            try:
                response = self.logsClient.describe_log_groups(logGroupNamePrefix=self.cwLogGroup)
                if len(response['logGroups']) == 0:
                    self.logsClient.create_log_group(logGroupName=self.cwLogGroup)
                    self.logsClient.put_retention_policy(
                                                logGroupName=self.cwLogGroup,
                                                retentionInDays=self.retentionDays
                                                )
            except ClientError as e:
                error_msg = e.response['Error']['Message']
                print("Error while creating logger - ", str(error_msg))
            except Exception as e:
                print("Error while creating logger - other exception ", str(e))

        try:
            if self.file:
                #Check if log folder exists, if not tries to create
                if not os.path.exists(loggingFolder):
                    os.makedirs(loggingFolder)
                logFilePath = '/'.join([loggingFolder, logFileName])
                #Check if log file exists, if it does opens to append, if not tries to create
                if os.path.exists(logFilePath):
                    self.logFile = open(logFilePath, 'a')
                    self.writeLog('Success', 'Log file opened:', logFilePath)
                else:
                    self.logFile = open(logFilePath, 'w+')
                    self.writeLog('Success', 'Log File created:', logFilePath)
            else:
                self.writeLog('Success', 'Log File skipped')
        except Exception as e:
            print("Error while creating logger", str(e))

    
    def kinesisPut(self, data):
        response = self.kinesisClient.describe_stream(StreamName=self.logStream)
        if len(response) > 0:
            self.kinesisClient.put_record(StreamName=self.logStream, Data=data, PartitionKey='mdmSyncLogger')
    
    ### Get log Stream
    ### second agurment is mandatory
    def getLogStream(self, logStreamPrefix):
        try:
            response = self.logsClient.describe_log_streams(
                                                logGroupName=self.cwLogGroup,
                                                logStreamNamePrefix=logStreamPrefix,
                                                descending=True
                                            )
            #creating or identifying log stream
            if 'logStreams' not in response.keys() or response['logStreams'] == []:
                logStreamNm=logStreamPrefix + '/' + datetime.datetime.now().strftime("%Y/%m/%d/%H%M%S")
                self.logsClient.create_log_stream(
                            logGroupName=self.cwLogGroup,
                            logStreamName=logStreamNm
                        )
                nextToken = None
            elif set(['lastIngestionTime','firstEventTimestamp']).issubset(response['logStreams'][0].keys()):
                logStreamNm = response['logStreams'][0]['logStreamName']
                lastIngestTime = response['logStreams'][0]['lastIngestionTime']
                firstEvenTime = response['logStreams'][0]['firstEventTimestamp']
                nextToken = response['logStreams'][0]['uploadSequenceToken']
                
                #if last log stream is having log events for more than 1 hour
                if (lastIngestTime-firstEvenTime)/1000>=3600:
                    logStreamNm=logStreamPrefix + '/' + datetime.datetime.now().strftime("%Y/%m/%d/%H%M%S")
                    self.logsClient.create_log_stream(
                                    logGroupName=self.cwLogGroup,
                                    logStreamName=logStreamNm
                                )
                    nextToken = None
            else:
                logStreamNm=response['logStreams'][0]['logStreamName']
                nextToken = None
            
            self.cwLogStream = logStreamNm
            return nextToken
        
        except ClientError as e:
            if (e.response['Error']['Code'] in ('ResourceAlreadyExistsException')):
                    response = self.logsClient.describe_log_streams(
                                logGroupName=self.cwLogGroup,
                                logStreamNamePrefix=logStreamPrefix,
                                descending=True
                                )
                    logStreamNm = response['logStreams'][0]['logStreamName']
                    nextToken = response['logStreams'][0]['uploadSequenceToken']                                
                                                    
                    self.cwLogStream = logStreamNm
                    return nextToken
            
        except Exception as e:
            print("Error while getting log stream information", str(e))


    ### writeLog
    ### This function writes the logs in the log file, it accepts multiple inputs to be logged, all is casted to text
    ### first agurment is mandatory and needs to be the log severity
    ### log format: <Date(YYYY-MM-DD)> <Time(HH24:MI:SS)> <Severity> <Message 1> <Message 2> .. <Message N> <line break>
    def writeLog(self, status, *args, nextToken=''):
        nextSeqToken = ''
        try:
            logTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logs = [self.logFileName, logTime, status]
            message = '"'
            for arg in args:
                message += ' ' + str(arg)
            message += '"'
            logs.append(message)
            logStr = '|'.join(logs).encode('utf-8').strip()
            
            if self.custom and not self.cwLogGroup == '' and not self.cwLogStream == '':
                putResponse={}
                if sys.getsizeof(logStr) <= 256000: 

                    i=1
                    while i<=10:  
                        try:
                            if nextToken is None:
                                putResponse = self.logsClient.put_log_events(
                                        logGroupName=self.cwLogGroup,
                                        logStreamName=self.cwLogStream,
                                        logEvents=[
                                                {
                                                    'timestamp': int(round(time.time() * 1000)),
                                                    'message': str(logStr)
                                                }
                                            ]
                                        )
                                break
                            else:
                                putResponse = self.logsClient.put_log_events(
                                                logGroupName=self.cwLogGroup,
                                                logStreamName=self.cwLogStream,
                                                logEvents=[
                                                    {
                                                        'timestamp': int(round(time.time() * 1000)),
                                                        'message': str(logStr)
                                                    }
                                                ],
                                                sequenceToken=nextToken
                                            )
                                break
                        except ClientError as e:
                            if (e.response['Error']['Code'] in ('InvalidSequenceTokenException','DataAlreadyAcceptedException')):
                                error_msg = e.response['Error']['Message']
                                #print('Debug','extracting next token from error message-',logStr)
                                nextToken=error_msg.split(":")[-1].strip()
                            else:
                                error_msg = e.response['Error']['Message']
                                print(logTime + '|' + 'Error' + '|' +'Getting Token:'+ str(error_msg))
                                break
                        except Exception as e:
                            print(logTime + '|' + 'Error' + '|'+ 'In putLogEvents:' + str(e)+str(logStr))
                            break
                        i += 1
                else:
                    print(logTime + '|' + 'Error' + '|' + 'Log stream too large exceeding 256Kb')
                
                if 'nextSequenceToken' in putResponse.keys():
                    nextSeqToken = putResponse['nextSequenceToken']
                else:
                    nextSeqToken='nextToken'
            else:
                print(str(logStr))
                
            if self.file and not self.logFileName == '':
                self.logFile.write(logStr.decode('utf-8'))
                self.logFile.write('\n')
                self.logFile.flush()
                
            if self.stream and not self.kinesisClient == None:
                objData = logStr + '\n'
                self.kinesisPut(objData)
                
            return nextSeqToken
        except Exception as e:
            print("Error while writing log", str(e))
        
    
    ### closeLog
    ### This function closes the log file, last operation performed by the script
    def closeLog(self):
        if not self.logFile == None:
            self.writeLog('Warning', 'Closing log file')
            self.logFile.close()
        else:
            self.writeLog('Warning', 'No file to close')

    
def connectAWS(logger, awsResource, AWSRegionName, AWSAccessKey, AWSSecretAccessKey, AWSSessionToken, proxy=None):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''
    try:
        next_token=logger.writeLog('Warning', 'Connecting to', awsResource, nextToken=next_token)
        if logger.logMode >= 1:
            next_token=logger.writeLog('Debug', 'Connection parameter AWSRegionName:', AWSRegionName, nextToken=next_token)
            next_token=logger.writeLog('Debug', 'Connection parameter AWSAccessKey:', AWSAccessKey, nextToken=next_token)
            next_token=logger.writeLog('Debug', 'Connection parameter AWSSecretAccessKey:', AWSSecretAccessKey, nextToken=next_token)
            next_token=logger.writeLog('Debug', 'Connection parameter AWSSessionToken:', AWSSessionToken, nextToken=next_token)
        if logger.logMode >= 2:
            next_token=logger.writeLog('Debug', 'Executing command:', 'S3Client = boto3.client(\'s3\', region_name = AWSRegionName, aws_access_key_id = AWSAccessKey,aws_secret_access_key = AWSSecretAccessKey, aws_session_token = AWSSessionToken)', nextToken=next_token)
        
        #------------------------------ Connects to S3 ------------------------------------                        
        if not proxy == None:
            if awsResource == 'S3':
                client = boto3.client('s3', 
                                      region_name = AWSRegionName, 
                                      aws_access_key_id = AWSAccessKey,
                                      aws_secret_access_key = AWSSecretAccessKey,
                                      aws_session_token = AWSSessionToken,
                                      config=Config(proxies=proxy))
            elif awsResource == 'kinesis':
                client = boto3.client('kinesis', region_name = AWSRegionName, 
                                             aws_access_key_id = AWSAccessKey,
                                             aws_secret_access_key = AWSSecretAccessKey,
                                             aws_session_token = AWSSessionToken,
                                      config=Config(proxies=proxy))
            elif awsResource == 'sqs':
                client = boto3.client('sqs', region_name = AWSRegionName, 
                                             aws_access_key_id = AWSAccessKey,
                                             aws_secret_access_key = AWSSecretAccessKey,
                                             aws_session_token = AWSSessionToken,
                                      config=Config(proxies=proxy))            
            elif awsResource == 'S3R':
                client = boto3.resource('s3', 
                                      region_name = AWSRegionName, 
                                      aws_access_key_id = AWSAccessKey,
                                      aws_secret_access_key = AWSSecretAccessKey,
                                      aws_session_token = AWSSessionToken,
                                      config=Config(proxies=proxy))
            elif awsResource == 'lambda':
                client = boto3.client('lambda', 
                                      region_name = AWSRegionName, 
                                      aws_access_key_id = AWSAccessKey,
                                      aws_secret_access_key = AWSSecretAccessKey,
                                      aws_session_token = AWSSessionToken,
                                      config=Config(proxies=proxy))
            elif awsResource == 'logs':                                         #Added for TestFramework: for custom logging
                client = boto3.client('logs', 
                                      region_name = AWSRegionName, 
                                      aws_access_key_id = AWSAccessKey,
                                      aws_secret_access_key = AWSSecretAccessKey,
                                      aws_session_token = AWSSessionToken,
                                      config=Config(proxies=proxy))
            elif awsResource == 'cloudwatch':                                   #Added for TestFramework: for telemetry
                client = boto3.client('cloudwatch', 
                                      region_name = AWSRegionName, 
                                      aws_access_key_id = AWSAccessKey,
                                      aws_secret_access_key = AWSSecretAccessKey,
                                      aws_session_token = AWSSessionToken,
                                      config=Config(proxies=proxy))

        else:
            if awsResource == 'S3':
                client = boto3.client('s3', 
                                      region_name = AWSRegionName, 
                                      aws_access_key_id = AWSAccessKey,
                                      aws_secret_access_key = AWSSecretAccessKey,
                                      aws_session_token = AWSSessionToken)
            elif awsResource == 'kinesis':
                client = boto3.client('kinesis', region_name = AWSRegionName, 
                                             aws_access_key_id = AWSAccessKey,
                                             aws_secret_access_key = AWSSecretAccessKey,
                                             aws_session_token = AWSSessionToken)
            elif awsResource == 'sqs':
                client = boto3.client('sqs', region_name = AWSRegionName, 
                                             aws_access_key_id = AWSAccessKey,
                                             aws_secret_access_key = AWSSecretAccessKey,
                                             aws_session_token = AWSSessionToken)
            elif awsResource == 'S3R':
                client = boto3.resource('s3', 
                                      region_name = AWSRegionName, 
                                      aws_access_key_id = AWSAccessKey,
                                      aws_secret_access_key = AWSSecretAccessKey,
                                      aws_session_token = AWSSessionToken)
            elif awsResource == 'lambda':
                client = boto3.client('lambda', 
                                      region_name = AWSRegionName, 
                                      aws_access_key_id = AWSAccessKey,
                                      aws_secret_access_key = AWSSecretAccessKey,
                                      aws_session_token = AWSSessionToken)
            elif awsResource == 'logs':                                         #Added for TestFramework: for custom logging
                client = boto3.client('logs', 
                                      region_name = AWSRegionName, 
                                      aws_access_key_id = AWSAccessKey,
                                      aws_secret_access_key = AWSSecretAccessKey,
                                      aws_session_token = AWSSessionToken)
            elif awsResource == 'cloudwatch':                                   #Added for TestFramework: for telemetry
                client = boto3.client('cloudwatch', 
                                      region_name = AWSRegionName, 
                                      aws_access_key_id = AWSAccessKey,
                                      aws_secret_access_key = AWSSecretAccessKey,
                                      aws_session_token = AWSSessionToken)
            
        logger.writeLog('Success', 'Connected to', awsResource, nextToken=next_token)
    except Exception as e:
        logger.writeLog('Error', 'Error found while connecting to', awsResource, e, nextToken=next_token)
        raise
    return client
    
### getCredentials
### This function executes the saml.py script to create the AWS credentials to access S3 and RDS
### this function is called every cycle
def getCredentials(logger, rootFolder, awsCredentialsFile):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''

    try:
        now =  datetime.datetime.utcnow()
        AWSRegionName ='eu-west-1'
        with open(awsCredentialsFile, 'r') as f:
            config = json.load(f)
        AWSAccessKey = config['saml']['awsAccessKeyId'] 
        AWSSecretAccessKey = config['saml']['awsSecretAccessKey']
        AWSSessionToken = config['saml']['awsSessionToken']
        AWSSessionExpire = datetime.datetime.strptime(config['saml']['expirationDate'], '%Y-%m-%dT%H:%M:%SZ')
        timediff = (AWSSessionExpire - now).seconds
        next_token=logger.writeLog('Warning', 'AWS credentials expiredate', AWSSessionExpire, timediff,'seconds remaining', nextToken=next_token)

        while timediff < 300 or timediff > 3600:
            time.sleep(60)
            with open(awsCredentialsFile, 'r') as f:
                config = json.load(f)
            AWSAccessKey = config['saml']['awsAccessKeyId'] 
            AWSSecretAccessKey = config['saml']['awsSecretAccessKey']
            AWSSessionToken = config['saml']['awsSessionToken']
            AWSSessionExpire = datetime.datetime.strptime(config['saml']['expirationDate'], '%Y-%m-%dT%H:%M:%SZ')
            timediff = (AWSSessionExpire - now).seconds
        else:
            next_token=logger.writeLog('Warning', 'Credentials are valid', nextToken=next_token)
        logger.writeLog('Success', 'Credentials created', nextToken=next_token)
    except Exception as e:
        logger.writeLog('Error', 'Error found while generating credentials:', e, nextToken=next_token)
    return AWSRegionName, AWSAccessKey, AWSSecretAccessKey, AWSSessionToken

### queryDB
### This function executes the SQL statement provided, if commit is True then the SQL needs to be UPDATE DML, otherwise SELECT DML
### It will fetch all the data and return to caller, if commit is True it will return the number of rows affected by UPDATE DML
def queryDB(logger, dbEngine, conn, sql, cur, commit=False, many=False, params=''):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''

    startTime = datetime.datetime.now()
    try:
        next_token=logger.writeLog('Warning', 'Querying', dbEngine, 'RDS', nextToken=next_token)
        if logger.logMode >= 1:
            next_token=logger.writeLog('Warning', 'Statement:', sql, nextToken=next_token)
        if many:
            if logger.logMode >= 1:
                next_token=logger.writeLog('Warning', 'Executing many SQL', nextToken=next_token)
            cur.executemany(sql, params)
        else:
            if logger.logMode >= 1:
                next_token=logger.writeLog('Warning', 'Executing standard SQL', nextToken=next_token)
            cur.execute(sql)
                 
        if commit:
            conn.commit()
            next_token=logger.writeLog('Warning', 'Query committed, records affected: ', cur.rowcount, nextToken=next_token)
            result = cur.rowcount
        else:
            if logger.logMode >= 1:
                next_token=logger.writeLog('Warning', 'Fetching all records', nextToken=next_token)
            result = cur.fetchall()
            next_token=logger.writeLog('Success', 'Query executed, row count:', cur.rowcount, nextToken=next_token)
    except Exception as e:
        next_token=logger.writeLog('Error', 'Error found while querying', dbEngine, 'RDS:', e, nextToken=next_token)
        logger.writeLog('Error', 'Query',sql, nextToken=next_token)
        raise
    endTime = datetime.datetime.now()
    next_token=logger.writeLog('Warning', 'Query',sql, nextToken=next_token)
    logger.writeLog('Warning', 'Query run in ', ((endTime - startTime).microseconds / 1000000), 'seconds', nextToken=next_token)
    return result

### connect
### This function closes the connection to RDS based, this function is called every cycle
def closeConn(logger, conn, dbEngine):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''

    try:
        next_token=logger.writeLog('Warning', 'Closing',dbEngine, 'RDS connection', nextToken=next_token)
        conn.close()
        logger.writeLog('Success', dbEngine, 'RDS connection close', nextToken=next_token)
    except Exception as e:
        logger.writeLog('Error', 'Error found while closing', dbEngine, 'RDS connection:', e, nextToken=next_token)


### This function connects to the RDS based on settings file, this function is called every cycle
def connect(logger, dbEngine, dbEndpoint, dbPort, dbMisc, dbUser, dbPwd):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''

    try:
        next_token=logger.writeLog('Warning', 'Connecting to',dbEngine, 'RDS', nextToken=next_token)
        if logger.logMode >= 1:
            next_token=logger.writeLog('Debug', 'Database parameters:', dbEndpoint, dbPort, dbMisc, nextToken=next_token)
        if logger.logMode >= 2:
            next_token=logger.writeLog('Debug', 'Executing command:', 'dsnStr = cx_Oracle.makedsn(dbEndpoint, dbPort, dbMisc[4:])', nextToken=next_token)
        dsnStr = cx_Oracle.makedsn(dbEndpoint, dbPort, dbMisc[4:])
        if logger.logMode >= 2:
            next_token=logger.writeLog('Debug', 'Executing command:', 'conn = cx_Oracle.connect(user=dbUser, password=dbPwd, dsn=dsnStr, encoding="UTF-8-SIG", nencoding="UTF-8")', nextToken=next_token)
        conn = cx_Oracle.connect(user=dbUser, password=dbPwd, dsn=dsnStr,  encoding="UTF-8", nencoding="UTF-8")
        logger.writeLog('Success', 'Connected to',dbEngine, 'RDS', nextToken=next_token)            
    except Exception as e:
        logger.writeLog('Error', 'Error found while connecting to database:', e, nextToken=next_token)
        conn = None
    return conn
###		


#SQS Queue operations
def sqsPut(logger, sqsClient, message, dedupId, queue):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''

    try:
        response = sqsClient.send_message(
            QueueUrl=queue,
            MessageBody=json.dumps(message)
            #MessageDeduplicationId=dedupId,
            #MessageGroupId='entityID'
            )
        logger.writeLog('Sucess', 'Message sent to SQS', nextToken=next_token)
        status = True
    except Exception as e:
        logger.writeLog('Error', 'Error found',  e, nextToken=next_token)
        status = False
    return status


# Put messages in target Queue in batch
def sqsPutMany(logger, sqsClient, messages, queue):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''

    try:
        i = 1
        entries = []
        l = len(messages)
        while i <= l:
            temp = {"Id":str(i), "MessageBody":json.dumps(messages[i-1])}
            entries.append(temp)
            if i % 10 == 0 or i == l:
                sqsClient.send_message_batch(
                    QueueUrl=queue,
                    Entries=entries
                    )

                next_token = logger.writeLog( "Warning", i, "messages sent from", l, nextToken=next_token)
                entries = []
            i += 1
        logger.writeLog('Sucess', 'Message sent to SQS', nextToken=next_token)
        status = True
    except Exception as e:
        logger.writeLog('Error', 'Error found',  e, nextToken=next_token)
        status = False
    return status


# Put messages in target Queue in batch and return status with failure message count
# Route single message to multiple Queues
# messageList => [{'queues':[], 'attr':{}, 'message':{}},...]      
def sqsPutManyAdv(logger, sqsClient, messageList):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''
    status=True
    success_cnt=0
    failure_cnt=0
    failed_msg_ids=[]
    try:
        entries = []
        queue_msg_map = {}
        i = 1
        l = len(messageList)
        next_token=logger.writeLog('Warning',str(l),'Messages received', nextToken=next_token)
        
        while i <= l:
            try:
                queue_list = messageList[i-1]['queues']
                temp = {"Id":str(i), "MessageBody":json.dumps(messageList[i-1]['message']), "MessageAttributes":messageList[i-1]['attr']}
                
                if sys.getsizeof(json.dumps(temp).encode('utf8')) <= 256000:
                    #To send a message to multiple queue
                    for queue in queue_list:
                        if queue in queue_msg_map.keys():
                            entries = queue_msg_map[queue]
                            if sys.getsizeof(json.dumps(entries).encode('utf8')) + sys.getsizeof(json.dumps(temp).encode('utf8')) <= 258000:
                                entries.append(temp)
                                queue_msg_map[queue] = entries
                                #if i % 10 == 0 or i == l:
                                if len(entries) == 10 or i == l:
                                    response=sqsClient.send_message_batch(
                                                        QueueUrl=queue,
                                                        Entries=entries
                                                    )
                                    success_cnt+=len(response['Successful'])
                                    if 'Failed' in response.keys():
                                        failure_cnt+=len(response['Failed'])
                                        for item in response['Failed']:
                                            failed_msg_ids.append(item['Id'])
                                        
                                    entries = []
                                    queue_msg_map[queue] = entries
                            else:
                                response=sqsClient.send_message_batch(
                                                        QueueUrl=queue,
                                                        Entries=entries
                                                    )
                                success_cnt+=len(response['Successful'])
                                if 'Failed' in response.keys():
                                    failure_cnt+=len(response['Failed'])
                                    for item in response['Failed']:
                                        failed_msg_ids.append(item['Id'])
                                        
                                entries = []
                                entries.append(temp)
                                queue_msg_map[queue] = entries
                        else:
                            lst=[]
                            lst.append(temp)
                            queue_msg_map[queue] = lst
                else:
                    next_token=logger.writeLog('Error','Message body too big.', nextToken=next_token)
                    next_token=logger.writeLog('Error','Payload size - ',sys.getsizeof(json.dumps(temp).encode('utf8')), nextToken=next_token)
                    next_token=logger.writeLog('Error','Discarded message - ',json.dumps(temp), nextToken=next_token)
                    failure_cnt+=1
                    failed_msg_ids.append(temp['Id'])
                    
                if i == l:
                    for queue in queue_msg_map.keys():
                        if len(queue_msg_map[queue])>0:
                            next_token = logger.writeLog( "Warning", "End of message list, message pending for ", queue, "number of messages left:", len(queue_msg_map[queue]), nextToken=next_token)
                            entries = queue_msg_map[queue]
                            response=sqsClient.send_message_batch(
                                                    QueueUrl=queue,
                                                    Entries=entries
                                                )
                            entries = []
                            success_cnt+=len(response['Successful'])
                            if 'Failed' in response.keys():
                                failure_cnt+=len(response['Failed'])
                                for item in response['Failed']:
                                    failed_msg_ids.append(item['Id'])
                i += 1 
                queue_list=[]
            except ClientError as e:
                next_token=logger.writeLog('Error',e, nextToken=next_token)
                next_token=logger.writeLog('Warning',str(len(entries)), 'Messages discarded', nextToken=next_token)
                #print(entries)
                #print(messageList[i-1])
                i+=1
                failure_cnt+=len(entries)
                for item in entries:
                    failed_msg_ids.append(item['Id'])
                queue_list=[]
                entries = []
                queue_msg_map[queue] = entries              
               
        logger.writeLog('Success', str(success_cnt) + ' records sent successfully and ' + str(failure_cnt) + ' records sent to queue failed', nextToken=next_token)
    except Exception as e:
        logger.writeLog('Error', 'Error found in sqsPutManyAdv module',  e, nextToken=next_token)
        status=False
        
    if len(failed_msg_ids)>0:
        failed_msg_ids = list(set(failed_msg_ids))
    return status,failed_msg_ids


# Get messages in batch from provided Queue
# Visibility timeout can be customized, default 300 sec
def sqsGet(logger,sqsClient,queue_url,timeOut=300):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''
    messages=[]
    try:
        # Receive message from SQS queue
        response = sqsClient.receive_message(
                QueueUrl=queue_url,
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=10,
                VisibilityTimeout=timeOut,
                WaitTimeSeconds=20
                )

        if 'Messages' in response.keys():
            for message in response['Messages']:
                if 'MessageAttributes' in message.keys():
                    msg={'messageId':message['MessageId'],'receiptHandle':message['ReceiptHandle'],'body':message['Body'], 'attributes':message['MessageAttributes']}
                else:
                    msg={'messageId':message['MessageId'],'receiptHandle':message['ReceiptHandle'],'body':message['Body']}
                messages.append(msg)
        return messages
    except Exception as e:
        logger.writeLog('Error', 'In sqsGet module', 'Not possible to fetch from SQS', e, nextToken=next_token)
        raise
        

# Delete messages from Sqs queue
# receipts -> list of message receipt handlers to be deleted
def deleteMessage(logger, sqsClient, queue_url, receipts):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''
    status = True
    dlt_process_cnt = {'dlt_success_cnt':0, 'dlt_fail_cnt':0}
    
    for receipt_handle in receipts:
        try:
            sqsClient.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
            dlt_process_cnt['dlt_success_cnt'] += 1
        except Exception as e:
            status = False
            next_token = logger.writeLog('Error', 'In deleteMessage module','Not able to delete from queue', e, nextToken=next_token)
            dlt_process_cnt['dlt_fail_cnt'] += 1
            
    logger.writeLog('Warning', str(dlt_process_cnt['dlt_success_cnt']) + ' messages deleted successfully and ' + str(dlt_process_cnt['dlt_fail_cnt']) + ' messages delete failed', nextToken=next_token)
    return status, dlt_process_cnt


# Delete messages from Sqs queue in batch
# receipts -> list of message receipt handlers to be deleted
def sqsDeleteMany(logger, sqsClient, queue_url, receipts):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''
    status = True
    dlt_process_cnt = {'dlt_success_cnt':0, 'dlt_fail_cnt':0}
    
    i = 1
    entries = []
    l = len(receipts)
    
    while i <= l:
        try:
            entries.append({'Id':str(i), 'ReceiptHandle':receipts[i-1]})
            if i%10 == 0 or i == l:
                response = sqsClient.delete_message_batch(QueueUrl=queue_url, Entries=entries)
                dlt_process_cnt['dlt_success_cnt'] += len(response['Successful'])
                if 'Failed' in response.keys():
                    dlt_process_cnt['dlt_fail_cnt'] += len(response['Failed'])
            
                entries.clear()
        except Exception as e:
            status = False
            next_token = logger.writeLog('Error', 'In sqsDeleteMany module','error while deleting from queue', e, nextToken=next_token)   
        i += 1
          
    logger.writeLog('Warning', str(dlt_process_cnt['dlt_success_cnt']) + ' messages deleted successfully and ' + str(dlt_process_cnt['dlt_fail_cnt']) + ' messages delete failed', nextToken=next_token)
    return status, dlt_process_cnt


def openSql(logger, filePath):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''

    try:
        with open(filePath, 'r') as f:
            sql = f.readlines()
            sql = ' '.join(sql).replace('\n','')
        logger.writeLog('Success', 'File', filePath, 'loaded', nextToken=next_token)
    except Exception as e:
        logger.writeLog('Error', 'Can not open file', filePath, nextToken=next_token)
        raise
    return sql


def downloadFromS3(logger, S3Client, bucketName, keyName, filePath):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''
    try:
        next_token=logger.writeLog('Warning', 'Initiating file transfer from S3 to localhost', nextToken=next_token)
        next_token=logger.writeLog('Warning', 'Transferring file from S3:', bucketName, keyName, nextToken=next_token)
        #------------------------------ Downloads from S3 ---------------------
        S3Client.Bucket(bucketName).download_file(keyName, filePath)
        logger.writeLog('Success', 'File transferred from S3:', bucketName, keyName, nextToken=next_token)
        status = True
    except Exception as e:
        logger.writeLog('Error', 'Error found while transferring file from S3:', bucketName, keyName, e, nextToken=next_token)
        status = False
    return status

            
def uploadToS3(logger, S3Client, bucketName, keyName, filePath):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''

    try:
        next_token=logger.writeLog('Warning', 'Initiating file transfer from  localhost to S3', nextToken=next_token)
        next_token=logger.writeLog('Warning', 'Transferring file to S3:', bucketName, keyName, nextToken=next_token)
        #------------------------------ Upload from S3 ---------------------
        S3Client.upload_file(filePath, Bucket = bucketName, Key = keyName)
        logger.writeLog('Success', 'File transferred to S3:', bucketName, keyName, nextToken=next_token)
        status = True
    except Exception as e:
        logger.writeLog('Error', 'Error found while transferring file to S3:', bucketName, keyName, e, nextToken=next_token)
        status = False
    return status

def updateLambda(logger, S3Client, bucketName, keyName, functionName):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''

    try:
        logger.writeLog('Warning', 'Initiating lambda update', nextToken=next_token)
        #------------------------------ Refresh lambda from S3 ---------------------
        S3Client.update_function_code(FunctionName=functionName, S3Bucket=bucketName, S3Key=keyName)
        status = True
    except Exception as e:
        logger.writeLog('Error', 'Error found while updating lambda:', bucketName, keyName, e, nextToken=next_token)
        status = False
    return status


def loadSql(file):
    with open(file,'r') as f:
        sql = f.readlines()
    sql = ''.join(sql).replace('\n',' ').replace('\t',' ')
    return sql


def putCWMetrics(logger,metrics,cloudwatchClient):
    if logger.custom:
        next_token = 'nextToken'
    else:
        next_token=''
    response = 'Metrics added'

    for metric in metrics:
        namespace = metric['namespace']
        metricName = metric['metricName']
        metricValue = metric['metricValue']
        dimension = metric['dimension']
        metricUnit = metric['metricUnit']

        dimensions = []
        for i, d in enumerate(dimension):
            temp = {
                'Name' : dimension[i]['name'],
                'Value' : dimension[i]['value']
            }
            dimensions.append(temp)
 
        try:
            # Put custom metrics
            metricData = [
                    {
                        'MetricName': metricName,
                        'Dimensions': dimensions,
                        'Value': metricValue,
                        'Timestamp' : datetime.datetime.now(),
                        'Unit': metricUnit
                    }
                ]
            #logger.writeLog('Debug', str(metricData))
            
            #Code to put metrics in cloudwatch
            cloudwatchClient.put_metric_data(
               MetricData = metricData,
              Namespace = namespace
            )
            logger.writeLog('Success', 'Metrics data added successfully', nextToken=next_token)
        except Exception as e:
            logger.writeLog('Error', 'Error while adding metrics data', str(e), nextToken=next_token)
            response = 'Metrics failed'

    return response


def createSQL(columns, table , condition = None):
    if type(columns) == list:
        columns = str(columns).replace("'", "")[1:-1]
    elif type(columns) == str:
        pass
    else:
        #print('Unktown datatype' + str(type(columns)))
        logger.writeLog('Warning', 'Unktown datatype' + str(type(columns)))
        raise
    sql = 'SELECT ' + columns + ' FROM ' + table
    if not condition == None:
        sql += ' WHERE ' + condition
    return sql


### getServiceStatus
### This function gets the status for this service from file SyncAWS.status located in settings folder
### this function is called every cycle
def getServiceStatus(logger, rootFolder, key):
    logger.writeLog('Warning', 'Checking SyncAWS service status')
    with open(rootFolder + '/settings/sync-aws.' + key + '.status', 'r') as f:
        data = json.load(f)
    serviceStatus = data['status']
    logger.writeLog('Warning', 'Service status is', serviceStatus)
    return serviceStatus


### startService
### This function sets the service status to "running" in SyncAWS.status file
def startService(logger, rootFolder, key):
    data = {"status" : "running"}
    logger.writeLog('Warning', 'Starting SyncAWS Service')
    with open(rootFolder + '/settings/sync-aws.' + key + '.status', 'w') as f:
        json.dump(data, f)


### stopService
### This function sets the service status to "stopped" in SyncAWS.status file
def stopService(logger, rootFolder, key):
    data = {"status" : "stopped"}
    logger.writeLog('Warning', 'Stopping SyncAWS Service')
    with open(rootFolder + '/settings/sync-aws.' + key + '.status', 'w') as f:
        json.dump(data, f)


### checkFolder
### This function check if the folder exists in NGDC before the donwload, if it doesn't the it tries to create
def checkFolder(logger, path):
    try:
        logger.writeLog('Warning', 'Verifying path in NGDC')
        path = path.split('\\')
        for i in range(1,len(path)+1):            
            testPath = '//'.join(path[0:i])
            if logger.logMode >= 1:
                logger.writeLog('Debug', 'Verifying path', testPath.replace('//','\\'))
            if not os.path.exists(testPath):
                if logger.logMode >= 1:
                    logger.writeLog('Debug','Path', testPath.replace('//','\\'), 'not found, trying to create')
                try:
                    os.makedirs(testPath)
                    logger.writeLog('Warning','Path', testPath.replace('//','\\'), 'created')
                except Exception as e:
                    logger.writeLog('Error','Error found while creating path', testPath.replace('//','\\'), e)
                    return False
        logger.writeLog('Success', 'Path verified in NGDC')                    
        return True
    except Exception as e:
        logger.writeLog('Error', 'Generic error found while cheking folders', e)
        return False


### checkFile
### This function check if the file exists in NGDC after the donwload, if it does the downloaded it returns True
def checkFile(logger, path):
    try:
        logger.writeLog('Warning','Verifying file', path)
        if os.path.exists(path):
            logger.writeLog('Success', 'File', path.replace('//','\\'), 'found')
            return True
        else:
            logger.writeLog('Error', 'File', path.replace('//','\\'), 'not found')
            return False
    except Exception as e:
        logger.writeLog('Error', 'Generic error found while cheking file', e)
        return False


def deleteFile(logger, path):
    try:
        logger.writeLog('Warning','Deleting file', path)
        if os.path.isfile(path):
            os.remove(path)
            logger.writeLog('Warning','File', path, 'deleted')
            return True
        else:
            logger.writeLog('Warning', path, 'is not a file')
            raise
    except Exception as e:
        logger.writeLog('Error', 'Generic error found while deleting file', e)
        return False


