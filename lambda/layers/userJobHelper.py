import json
import uuid
import constants
import boto3

import utilities
from utilities import JsonHelper
from utilities import Helper
from utilities import Logger
from utilities import LogLevel
from utilities import FileHelper
from utilities import FileNameOption
from botocore.exceptions import ClientError
from snsHelper import SnsHelper
from snsHelper import SnsS3Message
from dynamoDBHelper import DynamoDBHelper
from appConfigHelper import AppConfigHelper
from dynamoDBHelper import DynamoDBHelper


class UserJobHelper:
    
    appConfig = None
    overwriteNotes = False
    jobPrefix = constants.CHAR__EMPTY
    presentationTemplateFilename = constants.CHAR__EMPTY
    transcriptionsFilename = constants.CHAR__EMPTY
    slideTranscriptionsFilename = constants.CHAR__EMPTY
    slideTransitionsFilename = constants.CHAR__EMPTY
    slidePrefix = constants.CHAR__EMPTY
    videoFilename =  constants.CHAR__EMPTY
    presentationFilename =  constants.CHAR__EMPTY
    bucketname = constants.CHAR__EMPTY
    jobStatus = constants.CHAR__EMPTY
                
    def __init__(self, userEmailId: str, jobId = constants.CHAR__EMPTY, overwriteNotes = False):
        self.dynamoDb = DynamoDBHelper()
        self.tableName = constants.UGH_PC_DDB_TABLENAME__USER_JOBS
        self.overwriteNotes = overwriteNotes
        createNewJob = False
        if self.appConfig is None:
            self.appConfig = AppConfigHelper()
        if jobId is None or jobId == constants.CHAR__EMPTY:
            jobId = Helper.createGuid()            
            createNewJob = True
        elif self.loadJob(userEmailId, jobId) == False:
            createNewJob = True
        if createNewJob == True:
            Logger.log(f'CreateJob -> New JobId={jobId}')
            self.createJob(userEmailId, jobId, overwriteNotes)
            
    
    def loadJob(self, jobKeys: dict) -> bool:
        result = False
        if constants.UGH_PC_DDB_PK__USER_EMAIL_ID in jobKeys and constants.UGH_PC_DDB_SK__USER_JOB_ID in jobKeys:
            result = self.loadJob(jobKeys[constants.UGH_PC_DDB_PK__USER_EMAIL_ID], jobKeys[constants.UGH_PC_DDB_SK__USER_JOB_ID])
        return result
    
    
    def resolveTranscriptionsFilename(userEmailId: str, transcriptionsFilename: str) -> str:
        return transcriptionsFilename.replace(userEmailId, userEmailId.replace(constants.CHAR__AT, constants.CHAR__ASTERIX))
    
    
    def loadJob(self, userEmailId: str, jobId: str) -> bool:
        result = False
        Logger.log(f'LoadJob -> request = userEmailId={userEmailId}{constants.CHAR__NEWLINE} jobId={jobId}') 
        
        try:
            job = UserJobHelper.getJob(userEmailId, jobId)
            if job is None:
                Logger.log(f'LoadJob -> Failed to load job with jobId={self.jobId}{constants.CHAR__NEWLINE} for User={self.userEmailId}')
            else:
                self.userEmailId = job[constants.UGH_PC_DDB_PK__USER_EMAIL_ID]
                self.jobId = job[constants.UGH_PC_DDB_SK__USER_JOB_ID] 
                self.overwriteNotes = job[constants.UGH_PC_DDB_FLD__OVERWRITE_NOTES]
                self.bucketname = job[constants.UGH_PC_DDB_FLD__AC_BUCKETNAME]
                self.presentationTemplateFilename = job[constants.UGH_PC_DDB_FLD__JOB_ARTIFACTS][constants.UGH_PC_DDB_FLD__AC_PRESENTATION_TEMPLATE_FILENAME]
                self.transcriptionsFilename = job[constants.UGH_PC_DDB_FLD__JOB_ARTIFACTS][constants.UGH_PC_DDB_FLD__AC_TRANSCRIPTIONS_FILENAME]
                self.transcriptionsFilenameResolved = UserJobHelper.resolveTranscriptionsFilename(self.userEmailId, self.transcriptionsFilename)
                self.slideTranscriptionsFilename = job[constants.UGH_PC_DDB_FLD__JOB_ARTIFACTS][constants.UGH_PC_DDB_FLD__AC_SLIDE_TRANSCRIPTIONS_FILENAME ]
                self.slideTransitionsFilename = job[constants.UGH_PC_DDB_FLD__JOB_ARTIFACTS][constants.UGH_PC_DDB_FLD__AC_SLIDE_TRANSITIONS_FILENAME]
                self.slidePrefix = job[constants.UGH_PC_DDB_FLD__JOB_ARTIFACTS][constants.UGH_PC_DDB_FLD__AC_SLIDES_PREFIX]
                self.videoFilename =  job[constants.UGH_PC_DDB_FLD__AC_VIDEO_FILENAME]
                self.presentationFilename =  job[constants.UGH_PC_DDB_FLD__AC_PRESENTATION_FILENAME]
                self.jobStatus = job[constants.UGH_PC_DDB_FLD__JOB_STATUS]
                Logger.log(f'LoadJob -> Loaded Job={self}')
                result = True       
        except Exception as e:
             Logger.log(f'LoadJob -> ERROR = {e}', LogLevel.Exception)             
        return result
    
        
    def createJob(self, userEmailId: str, jobId: str, overwriteNotes = False):
        result = constants.CHAR__EMPTY
        if userEmailId is None or userEmailId == constants.CHAR__EMPTY:
            raise Exception(f"CreateJob -> User Email Id {userEmailId} cannot be null or empty")
        if jobId is None or jobId == constants.CHAR__EMPTY:
            raise Exception(f"CreateJob -> Job Id {jobId} cannot be null or empty")
        Logger.log(f'CreateJob -> request = userEmailId={userEmailId}{constants.CHAR__NEWLINE} jobId={jobId}{constants.CHAR__NEWLINE} overwriteNotes={overwriteNotes}')  
        try:
            self.userEmailId = userEmailId
            self.jobId = jobId    
            self.overwriteNotes = overwriteNotes            
            self.bucketname = self.appConfig.bucketname 
            self.jobPrefix = self.appConfig.jobPrefix.replace(constants.CHAR__PIPE + constants.UGH_PC_DDB_PK__USER_EMAIL_ID, self.userEmailId)
            self.jobPrefix = self.jobPrefix.replace(constants.CHAR__PIPE + constants.UGH_PC_DDB_SK__USER_JOB_ID, self.jobId)
            self.jobPrefix = self.jobPrefix.replace(constants.CHAR__PIPE + constants.UGH_PC_DDB_FLD__AC_BUCKET_PREFIX, self.appConfig.bucketPrefix)
            self.slidesPrefix = self.appConfig.slidesPrefix.replace(constants.CHAR__PIPE + constants.UGH_PC_DDB_FLD__AC_JOB_PREFIX, self.jobPrefix)
            self.jobPrefixPath = self.jobPrefix + constants.CHAR__FORWARD_SLASH 
            self.templatePrefixPath = self.appConfig.presentationTemplatePrefix.replace(constants.CHAR__PIPE + constants.UGH_PC_DDB_FLD__AC_BUCKET_PREFIX, self.appConfig.bucketPrefix) + constants.CHAR__FORWARD_SLASH 
            self.presentationTemplateFilename = self.templatePrefixPath + self.appConfig.presentationTemplateFilename
            self.transcriptionsFilename = self.jobPrefixPath + self.appConfig.transcriptionsFilename
            self.transcriptionsFilenameResolved = UserJobHelper.resolveTranscriptionsFilename(self.userEmailId, self.transcriptionsFilename)
            self.slideTranscriptionsFilename = self.jobPrefixPath + self.appConfig.slideTranscriptionsFilename
            self.slideTransitionsFilename = self.jobPrefixPath + self.appConfig.slideTransitionsFilename   
            job = UserJobHelper.getJob(userEmailId, jobId)
            
            if job is None:
                Logger.log(f'CreateJob -> Creating New Job with jobId={self.jobId}{constants.CHAR__NEWLINE} for User={self.userEmailId}')
                self.dynamoDb.putItem(self.tableName, 
                    item={
                        constants.UGH_PC_DDB_PK__USER_EMAIL_ID: self.userEmailId,
                        constants.UGH_PC_DDB_SK__USER_JOB_ID: self.jobId,
                        constants.UGH_PC_DDB_FLD__LAST_UPDATED: Helper.GetCurrentTime(),
                        constants.UGH_PC_DDB_FLD__AC_BUCKETNAME: self.bucketname,
                        constants.UGH_PC_DDB_FLD__OVERWRITE_NOTES: self.overwriteNotes,
                        constants.UGH_PC_DDB_FLD__JOB_STATUS:constants.UGH_PC_DDB_FLDVALUE__JOB_STATUS_STARTED
                    }
                )
                attributes = {
                                constants.UGH_PC_DDB_FLD__JOB_ARTIFACTS: {
                                    constants.UGH_PC_DDB_FLD__AC_PRESENTATION_TEMPLATE_FILENAME:self.presentationTemplateFilename,
                                    constants.UGH_PC_DDB_FLD__AC_SLIDE_TRANSITIONS_FILENAME:self.slideTransitionsFilename,
                                    constants.UGH_PC_DDB_FLD__AC_TRANSCRIPTIONS_FILENAME:self.transcriptionsFilename,
                                    constants.UGH_PC_DDB_FLD__AC_SLIDE_TRANSCRIPTIONS_FILENAME:self.slideTranscriptionsFilename,
                                    constants.UGH_PC_DDB_FLD__AC_SLIDES_PREFIX:self.slidesPrefix 
                                }
                            }
                Logger.log(f'CreateJob -> Updating Job Artifacts for jobId={self.jobId}{constants.CHAR__NEWLINE} attributes={attributes}')
                result = self.dynamoDb.updateItem(self.tableName, itemKey=UserJobHelper.getJobKey(self.userEmailId, self.jobId), attributes=attributes)
            else:
                Logger.log(f'CreateJob -> Retrieved Job={job}')
                
        except Exception as e:
            Logger.log(f'CreateJob -> ERROR = {e}', LogLevel.Exception)              
        return result
    
    
    def getJobKey(userEmailId: str, jobId: str):
        jobKey = {
            constants.UGH_PC_DDB_PK__USER_EMAIL_ID: userEmailId,
            constants.UGH_PC_DDB_SK__USER_JOB_ID: jobId      
        }
        return jobKey
    
    
    def getJobKeyFromFilename(filename: str):
        if filename is None or filename == constants.CHAR__EMPTY:
            raise Exception(f" GetJobKeyFromFilename -> Filename{filename} cannot be null or empty")
        filePath = FileHelper.getFileName(filename, FileNameOption.FilenamePath)
        jobId = FileHelper.getFileName(filename, FileNameOption.FilenameDirectory)
        userEmailId =  FileHelper.getFileName(filePath.replace(jobId, constants.CHAR__EMPTY), FileNameOption.FilenameDirectory)
        return UserJobHelper.getJobKey(userEmailId, jobId)
    
    
    def processVideoFileUploadEvent(snsEvent) -> bool:
        jobKeys = []
        Logger.log(f'ProcessVideoFileUploadEvent -> Event={snsEvent}')
               
        try:
            #Create an instance of the SNS helper class
            snsHelper = SnsHelper(boto3.Session().region_name)
            snsMessages = snsHelper.parseMessageFromS3Event(snsEvent)    
            for message in snsMessages:
                Logger.log(f'BucketName={message.bucketName}{constants.CHAR__NEWLINE} Key={message.key}')
                jobKeys.append(UserJobHelper.getJobKeyFromFilename(message.key))
        except Exception as e:
             Logger.log(f'GetJob -> ERROR = {e}', LogLevel.Exception)     
        return jobKeys
    
    
    def getJob(self):
        Logger.log(f'GetJob[instance] -> User={self.userEmailId}{constants.CHAR__NEWLINE} jobId={self.jobId}')
        return UserJobHelper.getJob(self.userEmailId, self.jobId)   
   
    
    def getJob(userEmailId: str, jobId: str):
        job = None
        Logger.log(f'GetJob[static] -> User={userEmailId}{constants.CHAR__NEWLINE} jobId={jobId}')
        
        try:
            dynamoDb = DynamoDBHelper()
            job = dynamoDb.getItem(constants.UGH_PC_DDB_TABLENAME__USER_JOBS, 
               itemKey=UserJobHelper.getJobKey(userEmailId, jobId), projectionExpression=constants.CHAR__EMPTY, 
                expressionAttributeNames=None, removeAttributeNames={ 'update_counter':'update_counter'})
        except Exception as e:
             Logger.log(f'GetJob -> ERROR = {e}', LogLevel.Exception)             
        return job
    
    
    def updateJobInputFiles(self, videoFilename: str, presentationFilename: str):
        Logger.log(f'updateJobInputFiles -> videoFilename={videoFilename}{constants.CHAR__NEWLINE} presentationFilename={presentationFilename}')
        
        try:
            self.videoFilename =  self.jobPrefixPath + videoFilename
            self.presentationFilename =  self.jobPrefixPath + presentationFilename
            self.dynamoDb.updateItem(self.tableName, 
            itemKey=UserJobHelper.getJobKey(self.userEmailId, self.jobId),
            attributes={
                constants.UGH_PC_DDB_FLD__AC_VIDEO_FILENAME:  self.videoFilename,
                constants.UGH_PC_DDB_FLD__AC_PRESENTATION_FILENAME:  self.presentationFilename
            })
        except Exception as e:
            Logger.log(f'UpdateJobInputFiles -> ERROR = {e}', LogLevel.Exception)  
            return False
        return True    
                                 
    
    def getJobStatus(self) -> str:
        self.loadJob(self.userEmailId, self.jobId)
        return self.jobStatus
    
    
    def updateJobStatus(self, jobStatus: str, jobStatusPrefix=constants.UGH_PC_DDB_FLDVALUE__STATUS_STARTED, updateOnlyWhenCurrentStatus=constants.CHAR__EMPTY):
        Logger.log(f'updateJobStatus -> jobStatus={jobStatus}{constants.CHAR__NEWLINE} jobStatusPrefix={jobStatusPrefix}{constants.CHAR__NEWLINE} currentStatus={updateOnlyWhenCurrentStatus}')
        
        try:
            updateStatus = True
            if updateOnlyWhenCurrentStatus != constants.CHAR__EMPTY:
                currentJobStatus = self.getJobStatus()
                updateStatus = True if currentJobStatus ==  updateOnlyWhenCurrentStatus else False
            if updateStatus == True:
                self.dynamoDb.updateItem(self.tableName, 
                itemKey=UserJobHelper.getJobKey(self.userEmailId, self.jobId),
                attributes={
                    constants.UGH_PC_DDB_FLD__JOB_STATUS: jobStatusPrefix + jobStatus
                })
        except Exception as e:
            Logger.log(f'UpdateJobStatus -> ERROR = {e}', LogLevel.Exception)  
            return False
        return True       