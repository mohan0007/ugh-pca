import boto3

import constants
import utilities
from utilities import Logger
from utilities import LogLevel
from datetime import datetime
from s3Helper import S3Helper
from sqsHelper import SqsHelper
from sqsHelper import SqsMessage
from userJobHelper import UserJobHelper


def lambda_handler(event, context):
    return processEvent(event)


def processEvent(snsEvent) -> bool:
    processedEvent = False
    jobKeys = UserJobHelper.processVideoFileUploadEvent(snsEvent)
    for jobKey in jobKeys:          
        userEmailId = jobKey[constants.UGH_PC_DDB_PK__USER_EMAIL_ID]
        jobId = jobKey[constants.UGH_PC_DDB_SK__USER_JOB_ID]
        processedEvent = processVideo(userEmailId, jobId)
    return processedEvent


def processVideo(userEmailId: str, jobId: str)-> bool:
    Logger.log(f'ProcessVideo -> request: userEmailId={userEmailId}{constants.CHAR__NEWLINE} jobId={jobId}')
    
    try:
        userJobHelper = UserJobHelper(userEmailId, jobId)
        Logger.log(f'ProcessVideo -> Deleting: transcriptionsFilename={userJobHelper.slideTransitionsFilename}')
        #s3Helper = S3Helper(boto3.Session().region_name)
        #s3Helper.deleteFile(userJobHelper.bucketname, userJobHelper.slideTransitionsFilename) 
        sqsHelper = SqsHelper(boto3.Session().region_name, userJobHelper.appConfig.ProcessVideoQueueName)
        message = SqsMessage(constants.UGH_PC_DDB_FLDVALUE__JOB_STATUS_PROCESSING_VIDEO, {
                                    constants.UGH_PC_DDB_PK__USER_EMAIL_ID:userJobHelper.userEmailId,
                                    constants.UGH_PC_DDB_SK__USER_JOB_ID: userJobHelper.jobId,
                                    constants.UGH_PC_DDB_FLD__AC_VIDEO_FILENAME:userJobHelper.videoFilename,
                                    constants.UGH_PC_DDB_FLD__AC_PRESENTATION_FILENAME:userJobHelper.presentationFilename
                                })
        Logger.log(f'ProcessVideo -> Sending Message={message} to Queue={userJobHelper.appConfig.ProcessVideoQueueName}')
        sqsHelper.sendMessage(sqsHelper.queueName, message)
    except Exception as e:
        Logger.log(f'ProcessVideo -> ERROR = {e}', LogLevel.Exception)    
        return False
    return True