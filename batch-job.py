import datetime
import json
import boto3
import os
import sys
import linecache
import time
import cx_Oracle
import sqlCSConfig as sq
#import sqlCSConfig1 as sq


# from DbLoaderInsert import dictRead

processStartTime = datetime.datetime.now()
# ------------------------------ Initialization --------------
rootFolder = os.path.abspath(os.path.dirname(__file__)) + '/'
try:
    with open('/tmp/HOSTALIASES', 'w') as hosts_file:
        hosts_file.write('{} localhost\n'.format(os.uname()[1]))
    tmpFolder = '/tmp/'
    env = 'aws'
except Exception as e:
    # rootFolder = '//e11flsxdmsemdata.pharma.aventis.com/xdm_sem_data/dev/scripts/platform/etl-api-mdm/v1/'
    tmpFolder = rootFolder + 'tmp/'
    env = 'local'

sys.path.append(rootFolder + 'utils')
import utils

# ------------------------------ Load Configuration File ----------------------------------------
with open(rootFolder + 'settings/mdm.config', 'r') as f:
    data = json.load(f)

# ------------------------------ Load General settings ----------------------------------------
AWSRegionName = data['AWSRegionName']
sqsEndpoint = data['SQSEndPointUrl']

# ------------------------------ Load Database settings ----------------------------------------
dbEngine = data['DBconf']['engine']
dbEndpoint = data['DBconf'][env + 'Endpoint']
dbPort = data['DBconf'][env + 'Port']
dbMisc = data['DBconf']['misc']
dbUser = data['DBconf']['user']
dbPwd = data['DBconf']['pwd']

# ------------------------------ Load path settings ----------------------------------------
loggingFolder = rootFolder + 'tmp/logs'
if env == 'local':
    awsCredentialsFile = data['paths']['AWScredential']
    proxy = data['proxy']

# ------------------------------ Load logging settings ---------------------------------------
logMode = int(data['logging']['logMode'])
logFileName = data['logging']['fileName'] + datetime.datetime.now().strftime("_%Y%m%d_%H%M%S.log")
file = bool(data['logging']['file'])
retentionDay = int(data['logging']['retentionPeriod'])
custom = bool(data['logging']['custom'])
logStream = data['logging']['logStream']
# logGroup = '/mdmSync'
# Logging Issue  Commented Not to use global logger
logger = utils.logger(logMode=logMode)
# if logger.custom:
#    logStreamPrefix = '/mdmsyncJsonParser'
#    next_token = logger.getLogStream(logStreamPrefix)
# else:
#    next_token = ''
# next_token = logger.writeLog('Warning', 'Config file path:', rootFolder + 'settings/mdm.config', nextToken=next_token)

# ------------------------------ Create SQS client -----------------------------------------
sqsClient = boto3.client('sqs', region_name=AWSRegionName,endpoint_url=sqsEndpoint)

# ------------------------------ Get SQL statements from local --------------

##This method is required to fetch CLOB object as String
def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)
    
def printException():
    global next_token
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    exp_details = 'Exception in ({}, Line {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)
    next_token = logger.writeLog('Error', 'Details :', exp_details, nextToken=next_token)


def getProcessTrack():
    global next_token
    try:
        resultSet = None
        conn = utils.connect(logger, dbEngine, dbEndpoint, dbPort, dbMisc, dbUser, dbPwd)
        cur = conn.cursor()

        sql = sq.sqlProcTrackQry
        #sql = sql.replace('<JSON_PROCESS_NM>', json_process_nm)

        resultSet = utils.queryDB(logger, dbEngine, conn, sql, cur)
        status = True

    except Exception as e:
        next_token = logger.writeLog('Error', 'Error on getProcessTrack function', e, nextToken=next_token)
        # printException(logObj)
        status = False

    return resultSet, status


def insProcessTrack(lastSK,ProcessTable):
    global next_token
    try:
        resultSet = None
        conn = utils.connect(logger, dbEngine, dbEndpoint, dbPort, dbMisc, dbUser, dbPwd)
        cur = conn.cursor()

        sql = sq.sqlProcTrackQryIns
        sql = sql.replace('<LAST_SK>', str(lastSK))
        sql = sql.replace('<TABLE_NM>', ProcessTable)

        resultSet = utils.queryDB(logger, dbEngine, conn, sql, cur,True)
        #utils.queryDB(logger, dbEngine, conn, sql, cur, True, True, jsonList) 
        status = True

    except Exception as e:
        next_token = logger.writeLog('Error', 'Error on insProcessTrack function', e, nextToken=next_token)
        # printException(logObj)
        status = False

    return resultSet, status


def updProcessTrack(lastSK,ProcessTable):
    global next_token
    try:
        resultSet = None
        conn = utils.connect(logger, dbEngine, dbEndpoint, dbPort, dbMisc, dbUser, dbPwd)
        cur = conn.cursor()

        sql = sq.sqlProcTrackQryUpd
        sql = sql.replace('<LAST_SK>', str(lastSK))
        sql = sql.replace('<TABLE_NM>', ProcessTable)

        resultSet = utils.queryDB(logger, dbEngine, conn, sql, cur,True)
        status = True

    except Exception as e:
        next_token = logger.writeLog('Error', 'Error on updProcessTrack function', e, nextToken=next_token)
        # printException(logObj)
        status = False

    return resultSet, status


def delProcessTrack():
    global next_token
    try:
        resultSet = None
        conn = utils.connect(logger, dbEngine, dbEndpoint, dbPort, dbMisc, dbUser, dbPwd)
        cur = conn.cursor()

        sql = sq.sqlProcTrackQryDel
        resultSet = utils.queryDB(logger, dbEngine, conn, sql, cur,True)
        status = True

    except Exception as e:
        next_token = logger.writeLog('Error', 'Error on delProcessTrack function', e, nextToken=next_token)
        # printException(logObj)
        status = False

    return resultSet, status

def modProcessTrack(lastSK,ProcessTable):
    global next_token
    try:
        resultSet = None
        status = True
        
        resultSet, status = getProcessTrack ()
        if len(resultSet) == 0 :
            next_token = logger.writeLog('Inform','INSERT Table CL_CS_PROCESS_TRACK with LastSK:',lastSK,'Process Table Name :' ,ProcessTable,nextToken=next_token)
            resultSet, status = insProcessTrack(lastSK,ProcessTable)
        else:
            next_token = logger.writeLog('Inform','UPDATE Table CL_CS_PROCESS_TRACK with LastSK:',lastSK,'Process Table Name :' ,ProcessTable,nextToken=next_token)
            resultSet, status = updProcessTrack(lastSK,ProcessTable)

    except Exception as e:
        next_token = logger.writeLog('Error', 'Error on modProcessTrack function', e, nextToken=next_token)
        # printException(logObj)
        status = False

    return resultSet, status




def getTableSKMinMax():
    global next_token
    try:
        resultSet = None
        conn = utils.connect(logger, dbEngine, dbEndpoint, dbPort, dbMisc, dbUser, dbPwd)
        cur = conn.cursor()

        sql = sq.sqlTableSKMinMax
        #sql = sql.replace('<JSON_PROCESS_NM>', json_process_nm)

        resultSet = utils.queryDB(logger, dbEngine, conn, sql, cur)
        status = True

    except Exception as e:
        next_token = logger.writeLog('Error', 'Error on getTableSKMinMax function', e, nextToken=next_token)
        # printException(logObj)
        status = False

    return resultSet, status


def getSrcTableData(srcTable,strtSK,endSK):
    global next_token
    try:
        resultSet = None
        conn = utils.connect(logger, dbEngine, dbEndpoint, dbPort, dbMisc, dbUser, dbPwd)
        conn.outputtypehandler = OutputTypeHandler
        cur = conn.cursor()
        sql = sq.srcTableQyery[srcTable]
        sql = sql.replace('<MIN_SK_NM>', str(strtSK))
        sql = sql.replace('<MAX_SK_NM>', str(endSK))

        #print(sql)

        resultSet = utils.queryDB(logger, dbEngine, conn, sql, cur)
        status = True

    except Exception as e:
        next_token = logger.writeLog('Error', 'Error on getSrcTableData function for query: ',srcTable, e, nextToken=next_token)
        # printException(logObj)
        status = False

    return resultSet, status



def srcRowDataFormatDict (rowData) :
    global next_token
    try:
        msgHeader = {}
        msgBody = {}
        msgCSJsonDict = {}
        msgHeader["countryIso2"] = rowData[1]
        msgHeader["applicationId"] = rowData[2]
        msgHeader["entity"] = rowData[4]
        msgHeader["mdmId"] = rowData[3]
        msgHeader["version"] = rowData[9]
        msgHeader["mdmChangeType"] = rowData[6]
        # Json text in body
        msgBody = rowData[8]
        
        #populate final sqs messages
        msgCSJsonDict ["header"] = msgHeader
        msgCSJsonDict ["body"] = json.loads(msgBody)
        
        #next_token = logger.writeLog('Inform', 'msgCSJsonDict ',msgCSJsonDict, nextToken=next_token)

        status = True

    except Exception as e:
            next_token = logger.writeLog('Error', 'Error on srcRowDataFormatDict function ',e, nextToken=next_token)
            # printException(logObj)
            status = False

    return msgCSJsonDict , status  


def srcDataProcess (srcTableResSet) :
    global next_token
    srcDataList = []
    srcDataDict = {}
    tableSK = 0
    try:
        for row in srcTableResSet :
            srcDataDict , status = srcRowDataFormatDict(row)
            srcDataList.append(srcDataDict)
            if (row[0] > tableSK ) :
                # Keep highest SK to track the data processing
                tableSK = row[0]
 
        status = True

    except Exception as e:
        next_token = logger.writeLog('Error', 'Error on srcDataProcess function ',e, nextToken=next_token)
        # printException()
        status = False

    return srcDataList, status , tableSK   



def putMessageCSQueue( csMessageList , queue_url):
    try:
        global next_token
        next_token = logger.writeLog('Inform','Processing Started for sending message in CS queue in putMessageCSQueue : ', nextToken=next_token)
        putCSMessageList = []
        put_msg_content = {}
        status = True
        # Prepare format for queue push
        
        for msg in csMessageList :
            put_msg_content = {}
            put_msg_content['queues'] = [queue_url]
            put_msg_content['attr'] = {}
            put_msg_content['message'] = msg
            putCSMessageList.append(put_msg_content)            
        
        status, failed_msg_ids = utils.sqsPutManyAdv(logger, sqsClient, putCSMessageList)
        if status == True:
            next_token = logger.writeLog('Inform', 'Success SQS Queue updated in putMessageCSQueue', nextToken=next_token)
        else:
            next_token = logger.writeLog('Error', 'Not possible to update to SQS in putMessageCSQueue', nextToken=next_token)
            status = False

        next_token = logger.writeLog('Inform','Processing End for sending message in CS queue in putMessageCSQueue : ', nextToken=next_token)

    except Exception as e:
        next_token = logger.writeLog('Error', 'Error on putMessageCSQueue : ', e, nextToken=next_token)
        status = False
    return status


def timeDiff(currentTime, StartTime):
    diffTime = (currentTime - StartTime)
    return (str(diffTime))



def csDataPopulateInit(event, context):
    try:
        global next_token
        target_queue = event['queue_url']
        batch_size = int(event['batch_size'])
        timeLimit = int(event['timeLimit'])        
        table_list = event['table_list']
        status = True
        stopFlag = True
        srcTable =None
        srcTableResSet = []
        srcTableStatus =None
        srcTableMinMaxDict = {}
        srcProcessTableLastSK = 0
        srcProcessTableName = None
        srcProcessTableList = []
        elementPos = 0
        inProgressFlag = False
        outerLoopBrkFlg = False

        

        if logger.custom:
            next_token = 'nextToken'
        else:
            next_token = ''

        processStartTime = datetime.datetime.now()
        processBeginTime = datetime.datetime.now()
        next_token = logger.writeLog('Inform', 'Start processing for  ======  csDataPopulateInit VER1',nextToken=next_token)
        next_token = logger.writeLog('Inform', 'target_queue :',target_queue,nextToken=next_token)
        next_token = logger.writeLog('Inform', 'batch_size :',batch_size,nextToken=next_token)
        next_token = logger.writeLog('Inform', 'timeLimit :',timeLimit,nextToken=next_token)
        next_token = logger.writeLog('Inform', 'table_list :',table_list,nextToken=next_token)
        # Get min and max table SK for processing in batch 
        resultSetMinMax, statusMinMax = getTableSKMinMax ()
        #print ('resultSetMinMax',resultSetMinMax)
        # Prepare dictionary for tracking Min Max SK
        for rec in resultSetMinMax :
            srcTableMinMaxDict [rec[0]] = {"min_sk":rec[1], "max_sk" :rec[2] }
        
        next_token = logger.writeLog('Inform', 'TABLE MIN MAX DETAILS ',srcTableMinMaxDict,nextToken=next_token)
        resultSet, status = getProcessTrack ()
        if ( status == False ):
            next_token = logger.writeLog('Error', 'Error in getProcessTrack ', nextToken=next_token)
            raise Exception("While processing data in getProcessTrack exception raised.")        
        next_token = logger.writeLog('Inform', 'PROCESS TRACK TABLE ENTRY ',resultSet,nextToken=next_token)
        
        # Check data is present in process track table ; if present then process start from there
        if len(resultSet) > 0 :
            srcProcessTableLastSK = resultSet[0][0]
            srcProcessTableName =  resultSet[0][1]
            elementPos = table_list.index(srcProcessTableName) # check which table was processing earlier and get the list position
            # create new list
            srcProcessTableList = table_list[elementPos:]
            inProgressFlag = True
            next_token = logger.writeLog('Inform', 'Previous processing Details','Table:',srcProcessTableName,'Previous process last SK',srcProcessTableLastSK,nextToken=next_token)
        else :
            srcProcessTableList = table_list
        
        
        #Processeing Start
        next_token = logger.writeLog('Inform', 'TABLE LIST TO PROCESS ',srcProcessTableList,nextToken=next_token)
        for tab in srcProcessTableList : 
            srcTable = tab
            minSK = srcTableMinMaxDict[tab]["min_sk"]
            maxSK = srcTableMinMaxDict[tab]["max_sk"]
            next_token = logger.writeLog('Inform', 'DETAILS OF TABLE : MIN : MAX ',tab,minSK,maxSK,nextToken=next_token)

            # assign previous processing details SK
            if (inProgressFlag == True ):
                strtSK = srcProcessTableLastSK
                runningSK = srcProcessTableLastSK
                inProgressFlag = False
            else :
                strtSK = minSK
                runningSK = minSK                
            

            stopFlag = True
            endSK = 0
            tabLatestSK = 0
            recCount = 0

            # Processing table data by batch and it is tracked by table SK
             # while loop Start 
            while stopFlag:
                if (datetime.datetime.now() - processStartTime).seconds < timeLimit and strtSK <= maxSK:
                    runningSK = runningSK + batch_size
                    if runningSK > maxSK :
                        runningSK = maxSK

                    endSK  = runningSK
                    #print ('maxSK',maxSK,'runningSK',runningSK,'strtSK',strtSK,'endSK',endSK)
                    srcTableResSet = {}
                    srcTableStatus = True
                    next_token = logger.writeLog('Inform', 'Data processing started for table:',tab,'strtSK:',strtSK,'endSK:',endSK, nextToken=next_token)
                    srcTableResSet, srcTableStatus = getSrcTableData (srcTable,strtSK,endSK)
                    if ( srcTableStatus == False ):
                        next_token = logger.writeLog('Error', 'Error in getSrcTableData for table:',tab,'strtSK:',strtSK,'endSK:',endSK, nextToken=next_token)
                        raise Exception("While processing data in getSrcTableData exception raised.")
                    
                    #Processing batch records
                    recCount = recCount + len(srcTableResSet) # Total recored fetched 
                    if (len(srcTableResSet) == 0 ) :
                        next_token = logger.writeLog('Inform', 'No records fetched for table:',tab,'strtSK:',strtSK,'endSK:',endSK, nextToken=next_token)
                        
                    else :
                        next_token = logger.writeLog('Inform', 'Processing records for table:',tab ,'strtSK:',strtSK,'endSK:',endSK, nextToken=next_token)
                        srcDataList = []
                        tabLatestSK = 0
                        status = True
                        # get the list of messahes for the source data set in CS writer format 
                        srcDataList ,status , tabLatestSK = srcDataProcess (srcTableResSet)
                        if ( status == False ):
                            next_token = logger.writeLog('Error', 'Error in srcDataProcess for table:',tab,'strtSK:',strtSK,'endSK:',endSK, nextToken=next_token)
                            raise Exception("While processing data in srcDataProcess exception raised.")                            
                        
                        #send the list of messages in CS queue
                        status = putMessageCSQueue(srcDataList,target_queue)
                        if ( status == False ):
                            next_token = logger.writeLog('Error', 'Error in putMessageCSQueue', nextToken=next_token)
                            raise Exception("While sending data to target queue from putMessageCSQueue exception raised.")
                        
                        # Update process table if message push is success 
                        resultSet, status = modProcessTrack(tabLatestSK,srcTable)
                        if ( status == False ):
                            next_token = logger.writeLog('Error', 'Error in modProcessTrack', nextToken=next_token)
                            raise Exception("While modifying data in process track table in modProcessTrack function  exception raised.")                        
                        #time.sleep(1)
                            
                    
                    strtSK = endSK + 1    
                    #print ('srcTableResSet', len(srcTableResSet))                            

                    
                else:
                    if ( strtSK > maxSK ) :
                        next_token = logger.writeLog('Inform', 'Data processing completed for table:',tab, 'strtSK:',minSK,'endSK:',maxSK, nextToken=next_token)
                        next_token = logger.writeLog('Inform', 'CLEANUP: DELETE CL_MDM.CL_CS_PROCESS_TRACK Table data', nextToken=next_token)
                        resultSet, status = delProcessTrack()
                    else:
                        next_token = logger.writeLog('Inform', ' TIME OUT : Data processing NOT completed for table:',tab, 'SK Processed strtSK:',minSK,'endSK:',endSK, nextToken=next_token)
                        outerLoopBrkFlg = True
                        
                    runTime = (datetime.datetime.now() - processStartTime).seconds
                    next_token = logger.writeLog('Inform', 'Execution Time : ',runTime ,' seconds','Total Record Processed for table:',tab, 'RecCount:',recCount, nextToken=next_token)
                    stopFlag = False

            
            # while loop End 
            # break outer for loop in case of time out
            if (outerLoopBrkFlg == True) :
                outerLoopBrkFlg = False
                break
                                  
        # for loop End        
        
        #raise Exception("While put messages in failure queue exception raised in jsonBulkAllModuleProcess.")

        status = True

    except Exception as e:
        next_token = logger.writeLog('Error', 'Error on csDataPopulateInit : ', e, nextToken=next_token)
        printException()
        status = False
    return status


#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Local Setup #########################
'''
context = {}
event = {}
event['queue_url'] = 'https://sqs.eu-west-1.amazonaws.com/421876295102/mdm-emea-sb-1'
event['batch_size'] = '1000'
event['timeLimit'] = '2'        
#event['table_list'] = ['AL_SEMARCHY.AL_O_MDM_HCP_JSON_DATA_HPJD','AL_SEMARCHY.AL_O_MDM_HCO_JSON_DATA_HOJD','AL_SEMARCHY.AL_O_MDM_AFF_JSON_DATA_AFJD']
#event['table_list'] = ['AL_SEMARCHY.AL_O_MDM_HCP_JSON_DATA_HPJD_TEST']
event['table_list'] = ['AL_SEMARCHY.AL_O_MDM_HCP_JSON_DATA_HPJD_TEST','AL_SEMARCHY.AL_O_MDM_HCO_JSON_DATA_HOJD_TEST','AL_SEMARCHY.AL_O_MDM_AFF_JSON_DATA_AFJD_TEST']
#event['table_list'] = ['AL_SEMARCHY.AL_O_MDM_HCP_JSON_DATA_HPJD']

status =  csDataPopulateInit(event, context)
'''
