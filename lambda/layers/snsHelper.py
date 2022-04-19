import os
import json
import uuid
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

import constants
from utilities import Logger
from utilities import LogLevel
from utilities import AttributeHelper
from utilities import AttributeDataTypes
from s3Helper import S3Helper


class SnsS3Message:
    
    def __init__(self, bucketName: str, key: str):
        self.bucketName = bucketName
        self.key = key
        

class SnsHelper:
    
    #File extension constants
    SNS__NAME = 'Sns'
    SNS__QUEUEURL = 'QueueUrl'
    
    def __init__(self, regionName: str, queueName=constants.CHAR__EMPTY):
        self.regionName = regionName
        #self.queueName = queueName
        #self.client = boto3.client(SqsHelper.SQS__NAME, self.regionName)    

    
    def parseMessageFromS3Event(self, snsEvent):
        result = []
        Logger.log(f'ParseMessageFromS3Event -> request: snsEvent={snsEvent}')
        
        if snsEvent != None:
            try:
                for record in snsEvent[constants.EVENT_METADATA__RECORDS]:
                    if SnsHelper.SNS__NAME in record:
                        if constants.EVENT_METADATA__MESSAGE in record[SnsHelper.SNS__NAME]:
                            message = record[SnsHelper.SNS__NAME][constants.EVENT_METADATA__MESSAGE]
                            for  messageRecord in message[constants.EVENT_METADATA__RECORDS]:
                                 if S3Helper.S3__NAME in messageRecord:
                                    if S3Helper.S3__BUCKET_LOWERCASE in messageRecord[S3Helper.S3__NAME]:
                                        if constants.EVENT_METADATA__NAME in messageRecord[S3Helper.S3__NAME][S3Helper.S3__BUCKET_LOWERCASE]:
                                            bucketName = messageRecord[S3Helper.S3__NAME][S3Helper.S3__BUCKET_LOWERCASE][constants.EVENT_METADATA__NAME]
                                    if S3Helper.S3__OBJECT in messageRecord[S3Helper.S3__NAME]:
                                        if S3Helper.S3__KEY_LOWERCASE in messageRecord[S3Helper.S3__NAME][S3Helper.S3__OBJECT]:
                                            key = messageRecord[S3Helper.S3__NAME][S3Helper.S3__OBJECT][S3Helper.S3__KEY_LOWERCASE]
                                    if bucketName != None and key != None:
                                        Logger.log(f'ParseMessageFromS3Event -> Appending objectKey=[{key}] found in bucket={bucketName}')
                                        result.append(SnsS3Message(bucketName, key))

            except Exception as e:
                Logger.log(f'ParseMessageFromS3Event -> ERROR = {e}', LogLevel.Exception)
            
        return result