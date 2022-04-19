import boto3
from botocore.exceptions import ClientError

import constants
from utilities import Logger
from utilities import LogLevel
from utilities import ListHelper
from utilities import AttributeHelper
from utilities import AttributeDataTypes


class SesEmailMessage:
    
    SES_MESSAGE__NAME = 'ses'
    SES_MESSAGE__BODY = 'Body'
    SES_MESSAGE__TEXT = 'Text'
    SES_MESSAGE__SUBJECT = 'Subject'
    SES_MESSAGE__DATA = 'Data'
    SES_MESSAGE__TOADDRESS = 'ToAddresses'
    SES_MESSAGE__CCADDRESS = 'CcAddresses'
    SES_MESSAGE__BCCADDRESS = 'BccAddresses'
    
        
    def __init__(self, source:str, destination, message, replyToAddresses):
        self.source = source
        self.destination = destination
        self.message = message
        self.replyToAddresses = ListHelper.createListFromString(replyToAddresses)
        
    
    def createMessage(messageSubject:str, messageBody:str):
        message = {
            SesEmailMessage.SES_MESSAGE__BODY: {
                SesEmailMessage.SES_MESSAGE__TEXT: {
                    constants.FORMAT_CHARSET: constants.FORMAT_CHARSET__UTF8,
                    SesEmailMessage.SES_MESSAGE__DATA: messageBody
                },
            },
            SesEmailMessage.SES_MESSAGE__SUBJECT: {
                constants.FORMAT_CHARSET: constants.FORMAT_CHARSET__UTF8,
                SesEmailMessage.SES_MESSAGE__DATA: messageSubject
            },
        }
        return message
    
    
    def createDestination(messageToAddresses:str, messageCcAddresses = None, messageBccAddresses = None):
        destination={
            SesEmailMessage.SES_MESSAGE__TOADDRESS: [
                messageToAddresses
            ]
        }
        if messageCcAddresses != None and messageCcAddresses != constants.CHAR__EMPTY:
            destination[SesEmailMessage.SES_MESSAGE__CCADDRESS] = ListHelper.createListFromString(messageCcAddresses)
        if messageBccAddresses != None and messageBccAddresses != constants.CHAR__EMPTY:
            destination[SesEmailMessage.SES_MESSAGE__BCCADDRESS] = ListHelper.createListFromString(messageBccAddresses)
        return destination
    
        
class SesHelper:
    
    #File extension constants
    SES__NAME = 'ses'

    
    def __init__(self, regionName: str):
        self.regionName = regionName
        self.client = boto3.client(SesHelper.SES__NAME, region_name=self.regionName)   

    
    def sendEmail(self, sesEmailMessage: SesEmailMessage):
        result = []
        Logger.log(f'SendEmail -> request: sesEmailMessage={sesEmailMessage}')

        if sesEmailMessage != None:
            try:
                response = self.client.send_email(Source=sesEmailMessage.source, Destination=sesEmailMessage.destination,
                    Message=sesEmailMessage.message, ReplyToAddresses=sesEmailMessage.replyToAddresses)        
                Logger.log(f'SendEmail -> response={response}')

            except Exception as e:
                Logger.log(f'SendEmail -> ERROR = {e}', LogLevel.Exception)
            
        return result  
    
    
    def sendCustomVerificationEmail(self, emailAddress:str, templateName:str, configurationSetName=None):
        result = None
        Logger.log(f'SendCustomVerificationEmail -> request: emailAddress={emailAddress}{constants.CHAR__NEWLINE} templateName={templateName}{constants.CHAR__NEWLINE} configurationSetName={configurationSetName}')
        
        if emailAddress != None and emailAddress != constants.CHAR__EMPTY and templateName != None and templateName != constants.CHAR__EMPTY:
            try:
                response = self.send_custom_verification_email(EmailAddress=emailAddress, TemplateName=templateName, ConfigurationSetName=configurationSetName)        
                Logger.log(f'SendCustomVerificationEmail -> response={response}')
                result = response
            except Exception as e:
                Logger.log(f'SendCustomVerificationEmail -> ERROR = {e}', LogLevel.Exception)                
        return result