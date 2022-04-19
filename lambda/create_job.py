import json
import boto3
import constants
import utilities

from userJobHelper import UserJobHelper
from s3Helper import S3Helper
from utilities import JsonHelper


def lambda_handler(event, context):
    
    body = json.loads(event[constants.HTTP_METADATA__BODY])
    
    if(constants.UGH_PC_DDB_FLD__AC_PRESENTATION_FILENAME in body):
        presentationFilename =  body[constants.UGH_PC_DDB_FLD__AC_PRESENTATION_FILENAME]

    if(constants.UGH_PC_DDB_FLD__OVERWRITE_NOTES in body):
        overwriteNotes = body[constants.UGH_PC_DDB_FLD__OVERWRITE_NOTES]

    responseBody = {
        constants.HTTP_METADATA__STATUS_CODE: constants.HTTP_METADATA__STATUS_CODE_SUCCESS,
        constants.HTTP_METADATA__BODY: createJob(body[constants.UGH_PC_DDB_PK__USER_EMAIL_ID], body[constants.UGH_PC_DDB_FLD__AC_VIDEO_FILENAME], presentationFilename, overwriteNotes)
    }

    results = {
        'statusCode' : 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': json.dumps(responseBody)
    }
    
    return results


def createJob(userEmailId: str, videoFilename: str, presentationFilename = constants.CHAR__EMPTY, overwriteNotes = False):
    # Create New Job
    userJobHelper = UserJobHelper(userEmailId, constants.CHAR__EMPTY, overwriteNotes)
    userJobHelper.updateJobInputFiles(videoFilename, presentationFilename)
    
    #Create an instance of the S3 helper class
    s3Helper = S3Helper(boto3.Session().region_name)
    videoUploadPresignedUrl = s3Helper.createUploadPresignedUrl(userJobHelper.bucketname, userJobHelper.videoFilename)
    if presentationFilename != None and presentationFilename != constants.CHAR__EMPTY:
        presentationUploadPresignedUrl = s3Helper.createUploadPresignedUrl(userJobHelper.bucketname, userJobHelper.presentationFilename)
    else:
        presentationUploadPresignedUrl = constants.CHAR__EMPTY
    
    return json.dumps({
        constants.UGH_PC_DDB_SK__USER_JOB_ID: userJobHelper.jobId,
        constants.UGH_PC_DDB_FLD__AC_VIDEO_FILENAME:  videoUploadPresignedUrl,
        constants.UGH_PC_DDB_FLD__AC_PRESENTATION_FILENAME:  presentationUploadPresignedUrl
    })