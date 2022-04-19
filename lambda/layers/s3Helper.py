import os
import json
import uuid
import boto3
from botocore.exceptions import ClientError

import constants
import utilities
from utilities import Helper
from utilities import Logger
from utilities import LogLevel
from utilities import FileHelper
from utilities import JsonHelper
from utilities import FileNameOption


class S3Helper:
    
    #S3 constants
    S3__NAME = 's3'
    S3__BUCKET = 'Bucket'
    S3__BUCKET_LOWERCASE = 'bucket'
    S3__KEY = 'Key'
    S3__KEYCOUNT = 'KeyCount'
    S3__KEY_LOWERCASE = 'key'
    S3__OBJECT = 'object'
    S3__GETOBJECT = 'get_object'
    S3__LISTOBJECTSV2 = 'list_objects_v2'
    S3__MAXITEMS = 'MaxItems'
    S3__PAGESIZE = 'PageSize'
    S3__CONTENTS = 'Contents'
    
                    
    def __init__(self, regionName: str):
        self.regionName = regionName
        self.client = boto3.client(S3Helper.S3__NAME, region_name=self.regionName)
        
    
    def getFiles(self, bucketName: str, prefix: str, maxItems=1, pageSize=1) -> bool:
        Logger.log(f'GetObjects -> request = bucketName={bucketName}{constants.CHAR__NEWLINE} prefix={prefix}{constants.CHAR__NEWLINE} maxItems={maxItems}{constants.CHAR__NEWLINE} pageSize={pageSize}')
        
        #Create S3 paginator object to list S3 objects
        paginator = self.client.get_paginator(S3Helper.S3__LISTOBJECTSV2)
        #Execute the paginate method
        return paginator.paginate(
            Bucket=bucketName,
            Prefix=prefix,
            # Set max items & page size to 1 for efficiency
            PaginationConfig={
                S3Helper.S3__MAXITEMS: maxItems,
                S3Helper.S3__PAGESIZE: pageSize
            })
    
    
    def prefixExists(self, bucketName: str, prefix: str) -> bool:
        prefixexists = False
        Logger.log(f'PrefixExists -> request = bucketName={bucketName}{constants.CHAR__NEWLINE} prefix={prefix}')
        
        #Execute the paginate method
        pages = self.getFiles(bucketName, prefix)
        # Loop through the page and make sure the KeyCount is > 0 [Will return 1 if prefix exists]
        for page in pages:
            prefixexists = page[S3Helper.S3__KEYCOUNT] > 0      
            break
        return prefixexists
    
    
    def getFullPath(bucketName: str, objectKey: str) -> str:
        return S3Helper.S3__NAME + constants.CHAR__COLON + constants.CHAR__FORWARD_SLASH + constants.CHAR__FORWARD_SLASH + bucketName + constants.CHAR__FORWARD_SLASH + objectKey
            
    
    def copyFile(self, sourceBucketName: str, sourceObjectKey: str, targetBucketName: str, targetObjectKey: str, removeSource = False) -> bool:
        Logger.log(f'CopyFile -> request: sourceBucketName={sourceBucketName}{constants.CHAR__NEWLINE} sourceObjectKey={sourceObjectKey} {constants.CHAR__NEWLINE} targetBucketName={targetBucketName} {constants.CHAR__NEWLINE} targetObjectKey={targetObjectKey}')
        
        try:
            self.client.copy({S3Helper.S3__BUCKET:sourceBucketName, S3Helper.S3__KEY:sourceObjectKey}, targetBucketName, targetObjectKey)
            if removeSource == True:
                self.deleteFile(sourceBucketName, sourceObjectKey) 
        except ClientError as e:
            Logger.log(f'CopyFile -> ERROR = {e}', LogLevel.Exception)  
            return False
        return True
        
        
    def downloadFile(self, bucketName: str, objectKey: str, downloadFilename: str) -> bool:
        # Download the file
        Logger.log(f'DownloadFile -> request: bucketName={bucketName}{constants.CHAR__NEWLINE} objectKey={objectKey} {constants.CHAR__NEWLINE} downloadFilename={downloadFilename}')
        
        try:
            self.client.download_file(bucketName, objectKey, downloadFilename)
        except ClientError as e:
            Logger.log(f'DownloadFile -> ERROR = {e}', LogLevel.Exception)  
            return False
        return True
    
    
    def downloadFiles(self, bucketName: str, prefix: str, downloadFoldername: str) -> bool:
        # Download the file
        Logger.log(f'DownloadFiles -> request: bucketName={bucketName}{constants.CHAR__NEWLINE} prefix={prefix} {constants.CHAR__NEWLINE} downloadFoldername={downloadFoldername}')
        
        try:
            downloadFoldername = downloadFoldername if downloadFoldername.endswith(constants.CHAR__FORWARD_SLASH ) else downloadFoldername + constants.CHAR__FORWARD_SLASH
            FileHelper.createDirectory(downloadFoldername) 
            #Execute the paginate method
            pages = self.getFiles(bucketName, prefix, 1000, 10)
             # Loop through the pages
            for page in pages:
                for keyObject in page[S3Helper.S3__CONTENTS]:
                    key = keyObject[S3Helper.S3__KEY]
                    if key.endswith(constants.CHAR__FORWARD_SLASH) == False:
                        self.downloadFile(bucketName, key, downloadFoldername + FileHelper.getFileName(key, FileNameOption.Filename))                
        except ClientError as e:
            Logger.log(f'DownloadFiles -> ERROR = {e}', LogLevel.Exception)  
            return False
        return True
   
    
    def uploadFile(self, bucketName: str, objectKey: str, fileName: str) -> bool:
         # If S3 objectKey was not specified, use file_name
        if objectKey is None:
            objectKey = os.path.basename(fileName)
        Logger.log(f'UploadFile -> request = bucketName={bucketName}{constants.CHAR__NEWLINE} objectKey={objectKey} {constants.CHAR__NEWLINE} fileName={fileName}') 
        # Upload the file
        try:
            response = self.client.upload_file(fileName, bucketName, objectKey)
        except ClientError as e:
            Logger.log(f'UploadFile -> ERROR = {e}', LogLevel.Exception) 
            return False
        return True
    
    
    def uploadFiles(self, bucketName: str, prefix: str, uploadFoldername: str) -> bool:
        # upload the files
        Logger.log(f'UploadFiles -> request: bucketName={bucketName}{constants.CHAR__NEWLINE} prefix={prefix} {constants.CHAR__NEWLINE} uploadFoldername={uploadFoldername}')
        
        try:
            files = FileHelper.getFiles(uploadFoldername)
            # Loop through the files
            for fileName in files:
                self.uploadFile(bucketName, prefix, fileName)
        except ClientError as e:
            Logger.log(f'UploadFiles -> ERROR = {e}', LogLevel.Exception)  
            return False
        return True
    
    
    def deleteFile(self, bucketName: str, objectKey: str) -> bool:
        Logger.log(f'DeleteFile -> request = bucketName={bucketName}{constants.CHAR__NEWLINE} objectKey={objectKey}') 
        # Delete the file
        try:
            response = self.client.delete_object(Bucket=bucketName, Key=objectKey)
        except ClientError as e:
            Logger.log(f'DeleteFile -> ERROR = {e}', LogLevel.Exception) 
            return False
        return True
    
    
    def createDownloadPresignedUrl(self, bucketName: str, objectKey: str, expiration=3600):
        
        Logger.log(f'CreateDownloadPresignedUrl -> request = bucketName={bucketName}{constants.CHAR__NEWLINE} objectKey={objectKey} {constants.CHAR__NEWLINE} expiration={expiration}')
        # Generate a presigned URL for the S3 object
        try:
            response = self.client.generate_presigned_url(S3Helper.S3__GETOBJECT,
                                                        Params={S3Helper.S3__BUCKET: bucketName,
                                                                S3Helper.S3__KEY: objectKey},
                                                        ExpiresIn=expiration)
            Logger.log(f'CreateDownloadPresignedUrl -> response={response}')
        except ClientError as e:
            Logger.log(f'CreateDownloadPresignedUrl -> ERROR = {e}', LogLevel.Exception)  
            return None

        # The response contains the presigned URL
        return response
    
  
    def createUploadPresignedUrl(self, bucketName: str, objectKey: str, fields=None, conditions=None, expiration=3600):

        Logger.log(f'CreateUploadPresignedUrl -> request = bucketName={bucketName}{constants.CHAR__NEWLINE} objectKey={objectKey} {constants.CHAR__NEWLINE} expiration={expiration}')
        # Generate a presigned URL for the S3 object
        try:
            response = self.client.generate_presigned_post(bucketName, objectKey, Fields=fields, Conditions=conditions, ExpiresIn=expiration)
            Logger.log(f'CreateUploadPresignedUrl -> response={response}')
        except ClientError as e:
            Logger.log(f'CreateUploadPresignedUrl -> ERROR = {e}', LogLevel.Exception)  
            return None

        # The response contains the presigned URL
        return response    