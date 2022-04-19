import json
import logging
import boto3
from botocore.exceptions import ClientError

import constants
import utilities
from utilities import Logger
from utilities import LogLevel
from s3Helper import S3Helper
from sqsHelper import SqsHelper
from sqsHelper import SqsMessage
from sesHelper import SesHelper
from sesHelper import SesEmailMessage
from userJobHelper import UserJobHelper


def lambda_handler(event, context):

    return processEvent(event)
    

def processEvent(sqsEvent) -> bool:
    processedEvent = False
    #Create an instance of the SQS helper class
    sqsHelper = SqsHelper(boto3.Session().region_name)
    sqsMessages = sqsHelper.parseMessageFromEvent(sqsEvent)
    
    for message in sqsMessages:
        Logger.log(f'ProcessEvent -> Messagebody={message.messageBody}{constants.CHAR__NEWLINE} MessageAttributes={message.messageAttributes}')
        if constants.UGH_PC_DDB_PK__USER_EMAIL_ID in message.messageAttributes:
            userEmailId = message.messageAttributes[constants.UGH_PC_DDB_PK__USER_EMAIL_ID]
            if constants.UGH_PC_DDB_SK__USER_JOB_ID in message.messageAttributes:
                jobId = message.messageAttributes[constants.UGH_PC_DDB_SK__USER_JOB_ID]
                if constants.UGH_PC_DDB_FLD__AC_PRESENTATION_FILENAME in message.messageAttributes:
                    presentationFilename = message.messageAttributes[constants.UGH_PC_DDB_FLD__AC_PRESENTATION_FILENAME]
                    Logger.log(f'ProcessEvent -> User={userEmailId}{constants.CHAR__NEWLINE} presentationFilename={presentationFilename}')
                    processedEvent = sendEmail(userEmailId, jobId, presentationFilename)
    return processedEvent
    
    
def sendEmail(userEmailId: str, jobId:str, presentationFilename:str) -> bool:
    region = boto3.Session().region_name
    
    try:
        # Create New Job when Job ID exists
        userJobHelper = UserJobHelper(userEmailId, jobId)
        Logger.log(f'SendEmail -> userJobHelper={userJobHelper}')
        
        #Create an instance of the S3 helper class
        s3Helper = S3Helper(region)
        presentationVideoFileUploadPresignedUrl = s3Helper.createDownloadPresignedUrl(userJobHelper.bucketname, presentationFilename)
        Logger.log(f'SendEmail -> presentationVideoFileUploadPresignedUrl={presentationVideoFileUploadPresignedUrl}')
        
        #Prepare Email Message
        messageSubject = f'[Attention] JobId: {jobId} completed. Your presentation is ready...' 
        messageBody = 'Download your presentation file here: ' + presentationVideoFileUploadPresignedUrl
        emailMessage = SesEmailMessage(userJobHelper.appConfig.SendEmailSourceName, SesEmailMessage.createDestination(userEmailId, constants.CHAR__EMPTY, constants.CHAR__EMPTY),
                                       SesEmailMessage.createMessage(messageSubject, messageBody), userJobHelper.appConfig.SendEmailSourceName)
        Logger.log(f'SendEmail -> emailMessage={emailMessage}')
        #Create an instance of the SES helper class
        sesHelper = SesHelper(region)
        sesHelper.sendEmail(emailMessage)
    except Exception as e:
        Logger.log(f'SendEmail -> ERROR = {e}', LogLevel.Exception)       
        return False
    return True