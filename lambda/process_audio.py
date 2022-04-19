import boto3

import constants
import utilities
from utilities import Logger
from utilities import LogLevel
from datetime import datetime
from s3Helper import S3Helper
from userJobHelper import UserJobHelper
from transcribeHelper import TranscribeHelper
from transcribeHelper import TranscribeJobParameters


def lambda_handler(event, context):
    return processEvent(event)


def processEvent(snsEvent) -> bool:
    processedEvent = False
    jobKeys = UserJobHelper.processVideoFileUploadEvent(snsEvent)
    for jobKey in jobKeys:          
        userEmailId = jobKey[constants.UGH_PC_DDB_PK__USER_EMAIL_ID]
        jobId = jobKey[constants.UGH_PC_DDB_SK__USER_JOB_ID]
        processedEvent = processAudio(userEmailId, jobId)
    return processedEvent


def processAudio(userEmailId: str, jobId: str)-> bool:
    Logger.log(f'ProcessAudio -> request: userEmailId={userEmailId}{constants.CHAR__NEWLINE} jobId={jobId}')
    
    try:
        userJobHelper = UserJobHelper(userEmailId, jobId)
        Logger.log(f'ProcessAudio -> Deleting: transcriptionsFilename={userJobHelper.transcriptionsFilenameResolved}')
        s3Helper = S3Helper(boto3.Session().region_name)
        s3Helper.deleteFile(userJobHelper.bucketname, userJobHelper.transcriptionsFilenameResolved) 
        transcribeJobParameters = TranscribeJobParameters(S3Helper.getFullPath(userJobHelper.bucketname, userJobHelper.videoFilename), userJobHelper.bucketname, userJobHelper.transcriptionsFilenameResolved)
        transcribeHelper = TranscribeHelper()
        transcribeJob = transcribeHelper.startTranscribeJob(transcribeJobParameters)
        Logger.log(f'ProcessAudio -> response: transcribeJob={transcribeJob}')
    except Exception as e:
        Logger.log(f'ProcessAudio -> ERROR = {e}', LogLevel.Exception)    
        return False
    return True