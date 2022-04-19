import os
import json
import uuid
import time
import boto3
import random

import logging
import constants
from enum import Enum
from pathlib import Path
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LogLevel(Enum):
    Notset = 0
    Verbose = 1
    Info = 2
    Warning = 3
    Exception = 4
    Critical = 5

    
class Helper:
   
    def createGuid():
        return str(uuid.uuid4())
    
    
    def GetCurrentTime():
        return str(datetime.now())
    
    
    def getFormattedCurrentTime():
        currentdatetime = datetime.now()
        return currentdatetime.strftime(constants.DATETIME_FORMAT__MON_DAY_YR_TIME)
    
    
    def sleepRandomSeconds(maximumSeconds: int):
        maximumSeconds = random.randrange(1, maximumSeconds, 1) if maximumSeconds > 0 else 1
        Helper.sleepBySeconds(maximumSeconds)
        return
    
    
    def sleepBySeconds(seconds: int):
        time.sleep(1000 * seconds)
        return
    
    
class ListHelper:    
    
    
    def createListFromString(inputValues, delimiter = constants.CHAR__COMMA):
        result = []
        if isinstance(inputValues,str):
            result.append(delimiter.join(inputValues.split()))
        elif isinstance(inputValues,list):
            result.append(delimiter.join(inputValues))
        else:
            raise Exception(f'Please provide a string or a list.')
        return(result)

    
class Logger:

    # Set logging level globally.
    defaultLogLevel = LogLevel.Info

    
    def setLogLevel (logLevel: LogLevel):
        defaultLogLevel = logLevel
        logger.setLevel(getLogLevel (logLevel))
    
    
    def getLogLevel (logLevel: LogLevel):
        result = logging.NOTSET
        if logLevel == LogLevel.Info:
            logging.INFO
        elif logLevel == LogLevel.Exception:
            logging.ERROR
        elif logLevel == LogLevel.Critical :
            logging.CRITICAL
        elif logLevel == LogLevel.Warning:
            logging.WARNING
        elif logLevel == LogLevel.Debug:
            logging.DEBUG
        return result
        
    
    def log(message: str, logLevel = defaultLogLevel):
        timeStampedLogMessage = f'{Helper.getFormattedCurrentTime()} - {message}{constants.CHAR__NEWLINE}'
        print(timeStampedLogMessage)
        
        if logLevel == LogLevel.Info or logLevel == LogLevel. Notset:
            logger.info(timeStampedLogMessage)
        elif logLevel == LogLevel.Exception:
            logger.error(timeStampedLogMessage)
        elif logLevel == LogLevel.Critical :
            logger.critical(timeStampedLogMessage)
        elif logLevel == LogLevel.Warning:
            logger.warning(timeStampedLogMessage)
        else:
            logger.debug(timeStampedLogMessage)
        return
    

class FileNameOption(Enum):
        Filename = 0
        FilenameOnly = 1
        FilenameExtension = 2
        FilenamePath = 3
        FilenameLastExtension = 4
        FilenameDirectory = 5

        
class FileHelper:
   
    def getFileName(filename: str, fileNameOption=FileNameOption.Filename):
        result = constants.CHAR__EMPTY
        if filename != None and filename != constants.CHAR__EMPTY: 
            fileBasename = os.path.basename(filename)
            filenameOnly = fileBasename .split(constants.CHAR__PERIOD)[constants.NUMBER__INDEX_ZER0]
            if fileNameOption == FileNameOption.Filename:
                result = fileBasename
            elif fileNameOption == FileNameOption.FilenameOnly:
                result = filenameOnly
            elif fileNameOption == FileNameOption.FilenameExtension:
                result = fileBasename.replace(filenameOnly, constants.CHAR__EMPTY)
            elif fileNameOption == FileNameOption.FilenameLastExtension:
                result = fileBasename.replace(Path(fileBasename).resolve().stem, constants.CHAR__EMPTY)
            elif fileNameOption == FileNameOption.FilenamePath:
                if fileBasename is None or fileBasename == constants.CHAR__EMPTY:
                    result = filename if filename.endswith(constants.CHAR__FORWARD_SLASH) else filename + constants.CHAR__FORWARD_SLASH
                else:
                    result = filename.replace(fileBasename, constants.CHAR__EMPTY)
            elif fileNameOption == FileNameOption.FilenameDirectory:           
                if filename.endswith(constants.CHAR__FORWARD_SLASH):
                    fileBasename = f'{filename[0: -1]}' if filename.endswith(constants.CHAR__FORWARD_SLASH) else filename
                else:
                    fileBasename = FileHelper.getFileName(filename, FileNameOption.FilenamePath)
                fileBasename = f'{fileBasename[0: -1]}' if fileBasename.endswith(constants.CHAR__FORWARD_SLASH) else fileBasename
                result = FileHelper.getFileName(fileBasename, FileNameOption.Filename)
        return result
   

    def getFiles(directoryName: str):
        return [files for files in os.listdir(directoryName) if os.path.isfile(os.path.join(directoryName, files))]
    
    
    def createDirectory(directoryName: str) -> bool:
        # Check whether the specified path exists or not
        isExist = os.path.exists(directoryName)
        if not isExist:
            # Create a new directory because it does not exist 
            os.makedirs(directoryName)
        return True
        

class JsonHelper:
   
    def jsonToDictionary(jsonString: str):
        jsonData = {}
        if jsonString != None:
            # returns JSON object as a dictionary
            jsonData = json.loads(jsonString)
        return jsonData
    
    
    def jsonFileToDictionary(jsonFilename: str):
        jsonFileData = None
        # Opening JSON file
        jsonFile = open(jsonFilename)
        # returns JSON object as a dictionary
        jsonFileData = json.load(jsonFile)
        return jsonFileData

    
    def jsonStringToJsonFile(jsonString: str, outputFilename = constants.CHAR__EMPTY) -> str:
        if outputFilename is None or outputFilename == constants.CHAR__EMPTY:
            outputFilename = str(uuid.uuid4()) + constants.CHAR__PERIOD + constants.FILE_EXTENSION__JSON
        # Opening JSON file
        jsonFile = open(outputFilename, constants.FILE_OPERATION__WRITE)
        jsonFile.write(jsonString)
        jsonFile.close()
        return outputFilename
    
    
class AttributeDataTypes(Enum):
    String = 0
    Binary = 1
    StringList = 2
    BinaryList = 3
    Custom = 4

    
class AttributeHelper:
    
    ATTRIBUTE_FLD__DATATYPE = 'DataType'
    ATTRIBUTE_FLD__VALUE = 'Value'
    ATTRIBUTE_FLD__VALUES = 'Values'
    ATTRIBUTE_ENUM__VALUE_PREFIX = 'AttributeDataTypes.'
    
    def resolveAttributeDataType(attributeDataType: AttributeDataTypes) -> str:
        return str(attributeDataType).replace(AttributeHelper.ATTRIBUTE_ENUM__VALUE_PREFIX, constants.CHAR__EMPTY)
    
    
    def resolveAttributeDataValueType(attributeDataType: AttributeDataTypes) -> str:
        attributeDataTypeValue = AttributeHelper.resolveAttributeDataType(attributeDataType)
        attributeValueKey = attributeDataTypeValue 
        
        if attributeDataType == AttributeHelper.resolveAttributeDataType(AttributeDataTypes.String):
            attributeValueKey = attributeValueKey + AttributeHelper.ATTRIBUTE_FLD__VALUE
        elif attributeDataType == AttributeHelper.resolveAttributeDataType(AttributeDataTypes.Binary):
            attributeValueKey = attributeValueKey + AttributeHelper.ATTRIBUTE_FLD__VALUE
        elif attributeDataType == AttributeHelper.resolveAttributeDataType(AttributeDataTypes.StringList):
            attributeValueKey = attributeValueKey + AttributeHelper.ATTRIBUTE_FLD__VALUES
        elif attributeDataType == AttributeHelper.resolveAttributeDataType(AttributeDataTypes.BinaryList):
            attributeValueKey = attributeValueKey + AttributeHelper.ATTRIBUTE_FLD__VALUES
        elif attributeDataType == AttributeHelper.resolveAttributeDataType(AttributeDataTypes.Custom):
            attributeValueKey = attributeValueKey + AttributeHelper.ATTRIBUTE_FLD__VALUE
        return attributeValueKey
    
    
    def createAttribute(attributes: dict, attributeName: str, attributeDataType: AttributeDataTypes, attributeValue) -> dict:
        if attributes is None:
            attributes = {}

        if attributeName != None and attributeName != constants.CHAR__EMPTY: 
            attributes[attributeName] = { AttributeHelper.ATTRIBUTE_FLD__DATATYPE: AttributeHelper.resolveAttributeDataType(attributeDataType), AttributeHelper.resolveAttributeDataValueType(attributeDataType): attributeValue}
        return attributes   
   
    
    def createAttributes(attributes: dict, attributeDataType = AttributeDataTypes.String) -> dict:
        result = {}
        attributeValue = None
        
        if attributes != None:
            for key, value in attributes.items():
                if type(value) is str:
                    attributeDataType = AttributeHelper.resolveAttributeDataType(AttributeDataTypes.String)
                    attributeValue = value
                elif AttributeHelper.ATTRIBUTE_FLD__DATATYPE in value:
                    attributeDataType = AttributeHelper.resolveAttributeDataType(value[AttributeHelper.ATTRIBUTE_FLD__DATATYPE])
                    if AttributeHelper.ATTRIBUTE_FLD__VALUE in value:
                        attributeValue = value[AttributeHelper.ATTRIBUTE_FLD__VALUE]
                result = AttributeHelper.createAttribute(result, key, attributeDataType, attributeValue)                
        return result
    
    
    def getAttributes(attributes: dict) -> dict:
        result = {}
        attributeValue = None
        attributeDataType = AttributeDataTypes.String
        
        if attributes != None:
            for key, value in attributes.items():
                if type(value) is str:
                    attributeValue = value
                elif AttributeHelper.ATTRIBUTE_FLD__DATATYPE in value:
                    attributeDataType = AttributeHelper.resolveAttributeDataType(value[AttributeHelper.ATTRIBUTE_FLD__DATATYPE])
                    attributeDataValueType = AttributeHelper.resolveAttributeDataValueType(attributeDataType)
                    if attributeDataValueType in value:
                        attributeValue = value[attributeDataValueType]
                result[key] = attributeValue                
        return result