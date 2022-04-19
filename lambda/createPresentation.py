import os
import boto3
#import uuid
import json
from time import gmtime, strftime, sleep

import constants
import utilities
from utilities import Logger
from utilities import LogLevel
from datetime import datetime
from s3Helper import S3Helper
from sqsHelper import SqsHelper
from sqsHelper import SqsMessage
from sesHelper import SesHelper
from sesHelper import SesEmailMessage
from utilities import FileHelper
from utilities import FileNameOption
from userJobHelper import UserJobHelper
from presentationHelper import PresentationHelper


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
                Logger.log(f'ProcessEvent -> User={userEmailId}{constants.CHAR__NEWLINE} jobId={jobId}')
                processedEvent = createPresentation(userEmailId, jobId)
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
    

def createPresentation(userEmailId: str, jobId: str) -> bool:
    createdPresentation = False
    Logger.log(f'CreatePresentation -> User={userEmailId}{constants.CHAR__NEWLINE} jobId={jobId}')
    
    try:
        # Create New Job when Job ID exists
        userJobHelper = UserJobHelper(userEmailId, jobId)
        Logger.log(f'CreatePresentation -> userJobHelper={userJobHelper}')

        userJobHelper.updateJobStatus(constants.UGH_PC_DDB_FLDVALUE__JOB_STATUS_CREATING_NOTES)
        os.chdir('/mnt/ugh')
        localDownloadPath = os.getcwd() + constants.CHAR__FORWARD_SLASH
        transcriptionsFilename=FileHelper.getFileName(userJobHelper.transcriptionsFilenameResolved, FileNameOption.Filename)
        slideTransistionsFilename = FileHelper.getFileName(userJobHelper.slideTransitionsFilename, FileNameOption.Filename)
        presentationFilename = FileHelper.getFileName(userJobHelper.presentationFilename, FileNameOption.Filename)
        createdPresentation = False
        Logger.log(f'CreatePresentation -> localDownloadPath={localDownloadPath}{constants.CHAR__NEWLINE} transcriptionsFilename={transcriptionsFilename}{constants.CHAR__NEWLINE} slideTransistionsFilename={slideTransistionsFilename}{constants.CHAR__NEWLINE} presentationFilename={presentationFilename}')
        region = boto3.Session().region_name
        s3Helper = S3Helper(region)
        continueFlag = False
        jobStatus = constants.UGH_PC_DDB_FLDVALUE__JOB_STATUS_PROCESSING_VIDEO
        
        if s3Helper.downloadFile(userJobHelper.bucketname, userJobHelper.slideTransitionsFilename, localDownloadPath + slideTransistionsFilename):
            if s3Helper.downloadFile(userJobHelper.bucketname, userJobHelper.transcriptionsFilename, localDownloadPath + transcriptionsFilename):
                slideTranscriptionsFilename = FileHelper.getFileName(userJobHelper.slideTranscriptionsFilename, FileNameOption.Filename)    
                presentationFileExtension = FileHelper.getFileName(userJobHelper.presentationTemplateFilename, FileNameOption.FilenameLastExtension)
                uploadPresentationFilename = FileHelper.getFileName(userJobHelper.videoFilename, FileNameOption.FilenamePath) + jobId + presentationFileExtension
                Logger.log(f'CreatePresentation -> slideTranscriptionsFilename={slideTranscriptionsFilename}{constants.CHAR__NEWLINE} presentationFileExtension={presentationFileExtension}{constants.CHAR__NEWLINE} uploadPresentationFilename={uploadPresentationFilename}')
                presentationHelper = PresentationHelper(slideTransistionsFilename, transcriptionsFilename)
                PresentationHelper.toJsonFile(presentationHelper.getNotes(), slideTranscriptionsFilename)
                userJobHelper.updateJobStatus(constants.UGH_PC_DDB_FLDVALUE__JOB_STATUS_CREATING_PRESENTATION)
                s3Helper.deleteFile(userJobHelper.bucketname, userJobHelper.slideTranscriptionsFilename)
                slidePrefix = FileHelper.getFileName(userJobHelper.slidePrefix, FileNameOption.Filename) + constants.CHAR__FORWARD_SLASH

                if userJobHelper.presentationFilename != constants.CHAR__EMPTY and s3Helper.downloadFile(userJobHelper.bucketname, userJobHelper.presentationFilename, localDownloadPath + presentationFilename):
                    Logger.log(f'CreatePresentation -> Downloaded Presentation Filename={userJobHelper.presentationFilename}')
                    continueFlag = True
                else:
                    Logger.log(f'CreatePresentation -> Trying to download all slide images from slides prefix={userJobHelper.slidePrefix} to Location={localDownloadPath + slidePrefix}')
                    if s3Helper.downloadFiles(userJobHelper.bucketname, userJobHelper.slidePrefix, localDownloadPath + slidePrefix):
                        presentationFilename = jobId + presentationFileExtension
                        if s3Helper.downloadFile(userJobHelper.bucketname, userJobHelper.presentationTemplateFilename, localDownloadPath + presentationFilename):
                            Logger.log(f'CreatePresentation -> Downloaded presentationTemplateFilename={userJobHelper.presentationTemplateFilename} as Presentation Filename={userJobHelper.presentationFilename}')
                            continueFlag = True

                if continueFlag:
                    addSlideNotesToPresentationArgs = {
                            constants.UGH_PC_DDB_FLD__AC_SLIDE_TRANSCRIPTIONS_FILENAME:  slideTranscriptionsFilename,
                            constants.UGH_PC_DDB_FLD__AC_PRESENTATION_FILENAME: presentationFilename,
                            constants.UGH_PC_DDB_FLD__AC_PRESENTATION_TEMPLATE_FILENAME: presentationFilename,
                            constants.UGH_PC_DDB_FLD__AC_SLIDES_PREFIX: slidePrefix,
                            constants.UGH_PC_DDB_FLD__OVERWRITE_NOTES : userJobHelper.overwriteNotes
                        }
                    if PresentationHelper.addSlideNotesToPresentation(addSlideNotesToPresentationArgs):
                        Logger.log(f'CreatePresentation -> Presentation notes added successfully - addSlideNotesToPresentationArgs={addSlideNotesToPresentationArgs}')
                        if s3Helper.uploadFile(userJobHelper.bucketname, userJobHelper.slideTranscriptionsFilename, localDownloadPath + slideTranscriptionsFilename) and s3Helper.uploadFile(userJobHelper.bucketname, uploadPresentationFilename, localDownloadPath + presentationFilename):
                            Logger.log(f'CreatePresentation -> Uploaded slideTranscriptionsFilename={userJobHelper.slideTranscriptionsFilename} and presentation={uploadPresentationFilename}')
                            createdPresentation = True
            else:
                Logger.log(f'CreatePresentation -> transcriptionsFilename={transcriptionsFilename} not found and will retry after sometime')
                jobStatus = constants.UGH_PC_DDB_FLDVALUE__JOB_STATUS_PROCESSING_AUDIO
        else:
            Logger.log(f'CreatePresentation -> slideTransistionsFilename={slideTransistionsFilename} not found and will retry after sometime')
            
        if continueFlag == True:
            userJobHelper.updateJobStatus(constants.UGH_PC_DDB_FLDVALUE__JOB_STATUS_CREATING_PRESENTATION,  constants.UGH_PC_DDB_FLDVALUE__STATUS_COMPLETE if createdPresentation else constants.UGH_PC_DDB_FLDVALUE__STATUS_FAILED)

            #Create an instance of the SQS helper class
            userJobHelper.updateJobStatus(constants.UGH_PC_DDB_FLDVALUE__JOB_STATUS_SENDING_EMAIL_NOTIFICATION)
            #sqsHelper = SqsHelper(region, userJobHelper.appConfig.SendEmailQueueName)
            #message = SqsMessage(constants.UGH_PC_DDB_FLDVALUE__JOB_STATUS_SENDING_EMAIL_NOTIFICATION, {
            #                            constants.UGH_PC_DDB_PK__USER_EMAIL_ID:userEmailId,
            #                            constants.UGH_PC_DDB_SK__USER_JOB_ID: jobId,
            #                            constants.UGH_PC_DDB_FLD__AC_PRESENTATION_FILENAME:presentationFilename
            #                       })
            #Logger.log(f'CreatePresentation -> Sending Message={message} to Queue={userJobHelper.appConfig.SendEmailQueueName}')
            #sqsHelper.sendMessage(sqsHelper.queueName, message)
            userJobHelper.updateJobStatus(constants.UGH_PC_DDB_FLDVALUE__JOB_STATUS_CREATING_PRESENTATION, constants.UGH_PC_DDB_FLDVALUE__STATUS_COMPLETE)
            #sendEmail(userEmailId, jobId, presentationFilename) 
            Logger.log(f'CreatePresentation -> {"Completed Successfully!!!" if createdPresentation else "Failed!!!"}')
        else:
            userJobHelper.updateJobStatus(jobStatus)
            #Reinsert the message into the Queue for processing it later
            sqsHelper = SqsHelper(region, userJobHelper.appConfig.CreatePresentationQueueName)
            message = SqsMessage(constants.UGH_PC_DDB_FLDVALUE__STATUS_RETRY + constants.UGH_PC_DDB_FLDVALUE__JOB_STATUS_CREATING_NOTES, {
                                        constants.UGH_PC_DDB_PK__USER_EMAIL_ID:userEmailId,
                                        constants.UGH_PC_DDB_SK__USER_JOB_ID: jobId
                                    })
            Logger.log(f'CreatePresentation -> Retry later: Sending Message={message} to Queue={userJobHelper.appConfig.SendEmailQueueName}')
            sqsHelper.sendMessage(sqsHelper.queueName, message)
            
    
    except Exception as e:
        Logger.log(f'CreatePresentation -> ERROR = {e}', LogLevel.Exception)       
   
    return createdPresentation