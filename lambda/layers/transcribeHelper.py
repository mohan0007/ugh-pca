import boto3
import requests
from botocore.exceptions import ClientError

import constants
from utilities import Helper
from utilities import Logger
from utilities import LogLevel
from utilities import FileHelper
from utilities import FileNameOption


class TranscribeJobParameters:
    
    def __init__(self, mediaUri: str, outputBucketName: str, outputKey: str, jobName=constants.CHAR__EMPTY, mediaFormat=constants.MEDIA_FILE_FORMAT__MP4, languageCode=constants.LANGUAGE_CODE__ENGLISH_US, vocabularyName=None):
        if mediaUri is None or mediaUri == constants.CHAR__EMPTY:
            raise Exception(f"Create Transcribe Job Parameters -> Media Uri {mediaUri} cannot be null or empty")
        self.mediaUri = mediaUri
        self.jobName = (FileHelper.getFileName(mediaUri, FileNameOption.FilenameOnly) + constants.CHAR__UNDSERSCORE + Helper.getFormattedCurrentTime()).replace(constants.CHAR__SPACE, constants.CHAR__UNDSERSCORE).replace(constants.CHAR__COLON, constants.CHAR__PERIOD) if jobName is None or jobName == constants.CHAR__EMPTY else jobName       
        self.mediaFormat = mediaFormat
        self.languageCode = languageCode
        self.vocabularyName = vocabularyName
        self.outputBucketName = outputBucketName
        self.outputKey = outputKey

        
class TranscribeHelper:
    
    #File extension constants
    TRANSCRIBE__NAME = 'transcribe'
    TRANSCRIBE__JOB = 'TranscriptionJob'
    TRANSCRIBE__JOBNAME = 'TranscriptionJobName'
    TRANSCRIBE__VOCABULARY_NAME = 'VocabularyName'
    TRANSCRIBE__JOB_OUTPUT_BUCKET = 'OutputBucketName'
    TRANSCRIBE__JOB_OUTPUT_KEY = 'OutputKey'

    def __init__(self):
        self.client = boto3.client(TranscribeHelper.TRANSCRIBE__NAME)    
        

    def startTranscribeJob(self, transcribeJobParameters: TranscribeJobParameters):
        Logger.log(f'StartTranscribeJob -> request: TranscribeJobParameters={transcribeJobParameters}')

        try:
            jobParameters = {
                TranscribeHelper.TRANSCRIBE__JOBNAME: transcribeJobParameters.jobName,
                constants.MEDIA: {constants.MEDIA_FILE_URI: transcribeJobParameters.mediaUri},
                constants.MEDIA_FILE_FORMAT: transcribeJobParameters.mediaFormat,
                constants.LANGUAGE_CODE:  transcribeJobParameters.languageCode,
                TranscribeHelper.TRANSCRIBE__JOB_OUTPUT_BUCKET: transcribeJobParameters.outputBucketName,
                TranscribeHelper.TRANSCRIBE__JOB_OUTPUT_KEY: transcribeJobParameters.outputKey
            }
            if transcribeJobParameters.vocabularyName is not None:
                jobParameters[constants.CONFIGURATION__SETTINGS] = {TranscribeHelper.TRANSCRIBE__VOCABULARY_NAME: transcribeJobParameters.vocabularyName}
            Logger.log(f'StartTranscribeJob -> request: jobParameters={jobParameters}')
            response = self.client.start_transcription_job(**jobParameters)
            job = response[TranscribeHelper.TRANSCRIBE__JOB]
            Logger.log(f'StartTranscribeJob -> Job Started: response={response}')
        except ClientError as e:
            Logger.log(f'StartTranscribeJob -> ERROR = {e}', LogLevel.Exception)            
        else:
            return job