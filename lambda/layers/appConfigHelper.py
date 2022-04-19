import json
import boto3

import constants
import utilities
from utilities import Logger
from utilities import JsonHelper
from botocore.exceptions import ClientError
from dynamoDBHelper import DynamoDBHelper


class AppConfigHelper:
    
    def __init__(self, applicationId=constants.UGH_PC__APPLICATION_ID, applicationCode=constants.UGH_PC__APPLICATION_CODE):
        self.applicationId = applicationId
        self.applicationCode = applicationCode
        self.tableName = constants.UGH_PC_DDB_TABLENAME__APP_CONFIG
        itemkey = {
                constants.UGH_PC_DDB_FLD__AC_APPLICATION_ID: self.applicationId
        }
        Logger.log(f'{constants.UGH_PC_DDB_FLD__AC_APPLICATION_ID}={self.applicationId}{constants.CHAR__NEWLINE} {constants.UGH_PC_DDB_FLD__AC_APPLICATION_CODE}={self.applicationCode}{constants.CHAR__NEWLINE} Tablename={self.tableName}{constants.CHAR__NEWLINE} itemKey={itemkey}')
        self.dynamoDb = DynamoDBHelper()
        self.appConfig =  self.dynamoDb.getItem(self.tableName, itemKey=itemkey)
        self.presentationTemplateFilename = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_PRESENTATION_TEMPLATE_FILENAME)
        self.presentationTemplatePrefix = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_TEMPLATES_PREFIX)
        self.transcriptionsFilename = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_TRANSCRIPTIONS_FILENAME)
        self.slideTranscriptionsFilename = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_SLIDE_TRANSCRIPTIONS_FILENAME)
        self.bucketname = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_BUCKETNAME)
        self.jobPrefix = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_JOB_PREFIX)
        self.slidesPrefix = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_SLIDES_PREFIX)
        self.bucketPrefix = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_BUCKET_PREFIX)
        self.slideTransitionsFilename = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_SLIDE_TRANSITIONS_FILENAME)        
        self.CreatePresentationQueueName = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_CREATE_PRESENTATION_QUEUENAME)
        self.SendEmailQueueName = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_SEND_EMAIL_QUEUENAME)
        self.ProcessVideoQueueName = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_PROCESS_VIDEO_QUEUENAME)
        self.SendEmailSourceName = self.getAppConfigValue(constants.UGH_PC_DDB_FLD__AC_SEND_EMAIL_SOURCENAME)
        Logger.log(f'AppConfig = {self.appConfig}')
        

    def getAppConfigValue(self, configId: str):
        result = constants.CHAR__EMPTY
        try:
            if configId in self.appConfig:
                result = self.appConfig[configId]
        except Exception as e:
            Logger.log(f'GetAppConfigValue -> result = {result}', LogLevel.Exception)
            return result
        return result     