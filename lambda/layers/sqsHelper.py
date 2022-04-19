import os
import json
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

import constants
from utilities import Logger
from utilities import LogLevel
from utilities import AttributeHelper
from utilities import AttributeDataTypes


class SqsMessage:
    
    def __init__(self, messageBody: str, messageAttributes: dict):
        self.messageBody = messageBody
        self.messageAttributes = messageAttributes
        

class SqsHelper:
    
    #File extension constants
    SQS__NAME = 'sqs'
    SQS__QUEUEURL = 'QueueUrl'
    SQS__MESSAGE_ALL = 'All'
    SQS__MESSAGE_ATTRIBUTES = 'messageAttributes'

    
    def __init__(self, regionName: str, queueName=constants.CHAR__EMPTY):
        self.regionName = regionName
        self.queueName = queueName
        self.client = boto3.client(SqsHelper.SQS__NAME, self.regionName)
        
    
    def getQueueByName(self, queueName: str):
        Logger.log(f'GetQueueByName -> request = queueName={queueName}')
        
        try:
            response = self.client.get_queue_url(
                QueueName=queueName,
            )
            Logger.log(f'GetQueueByName -> response = response={response}')
            return response[SqsHelper.SQS__QUEUEURL]

        except ClientError as e:
            Logger.log(f'GetQueueByName -> ERROR = {e}', LogLevel.Exception)            
            return None
    
    
    # @dispatch(str, str, dict)
    # def sendMessage(self, queueName: str, messageBody: str, messageAttributes: dict):
        # Logger.log(f'SendMessage -> request  queueName={queueName}{constants.CHAR__NEWLINE} messageBody={messageBody}{constants.CHAR__NEWLINE} messageAttributes={messageAttributes}')
        
        # try:
             # response = self.client.send_message(
                # QueueUrl = self.getQueueByName(queueName),
                # MessageBody = messageBody,
                # MessageAttributes = messageAttributes
        # )
        # except ClientError as e:
            # Logger.log(f'SendMessage -> ERROR = {e}', LogLevel.Exception)            
        # return response          
    
    
    def sendMessage(self, queueName: str, message: SqsMessage):
        Logger.log(f'SendMessage -> request  queueName={queueName}{constants.CHAR__NEWLINE} message={message}')
        response = None
        
        if message != None:
            try:
                response = self.client.send_message(
                    QueueUrl = self.getQueueByName(queueName),
                    MessageBody = message.messageBody,
                    MessageAttributes = AttributeHelper.createAttributes(message.messageAttributes)
                )
            except ClientError as e:
                Logger.log(f'SendMessage -> ERROR = {e}', LogLevel.Exception)            
        return response
    
    
    def receiveMessage(self, queueName:str, messageAttributeNames, maxNumberOfMessages=1, waitTimeSeconds=10):
        Logger.log(f'ReceiveMessage -> request  queueName={queueName}{constants.CHAR__NEWLINE} maxNumberOfMessages={maxNumberOfMessages}{constants.CHAR__NEWLINE} waitTimeSeconds={waitTimeSeconds}')
        response = {}
        
        try:
             response = self.client.receive_message(
                QueueUrl=self.getQueueByName(queueName),
                AttributeNames=SqsHelperSQS__MESSAGE_ALL,
                MessageAttributeNames=messageAttributeNames,
                MaxNumberOfMessages=maxNumberOfMessages,
                WaitTimeSeconds=waitTimeSeconds
        )
        except ClientError as e:
            Logger.log(f'ReceiveMessage -> ERROR = {e}', LogLevel.Exception)
        return response  
    
    
    def purgeQueue(self, queueName: str):
        Logger.log(f'PurgeQueue -> request = queueName={queueName}')

        try:
            response = self.client.purge_queue(QueueUrl=self.getQueueByName(queueName))

        except ClientError:
            Logger.log(f'PurgeQueue -> ERROR = {e}', LogLevel.Exception)
        return response
    
    
    def parseMessageFromEvent(self, sqsEvent):
        result = []
        Logger.log(f'ParseMessageFromEvent -> request: sqsEvent={sqsEvent}')
        
        if sqsEvent != None:
            try:
                for record in sqsEvent[constants.EVENT_METADATA__RECORDS]:
                    result.append(SqsMessage(record[constants.EVENT_METADATA__BODY], AttributeHelper.getAttributes(record[SqsHelper.SQS__MESSAGE_ATTRIBUTES])))

            except Exception as e:
                Logger.log(f'ParseMessageFromEvent -> ERROR = {e}', LogLevel.Exception)
            
        return result