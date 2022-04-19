import os
import json
import uuid
import boto3

import constants
from utilities import JsonHelper
from utilities import Logger
from utilities import LogLevel
from datetime import datetime
from botocore.exceptions import ClientError


class DynamoDBHelper:
    
    #File extension constants
    DDB__NAME = 'dynamodb'
    DDB__ITEM = 'Item'
    DDB__ITEMS = 'Items'
    DDB__SET = 'SET'
    DDB_RETURNVALUES__ALL_OLD = 'ALL_OLD'
    DDB_FLD__LAST_UPDATED = 'last_updated'
    
    def __init__(self, regionName=constants.CHAR__EMPTY):
        self.regionName = regionName
        self.client = boto3.resource(DynamoDBHelper.DDB__NAME)
        
    
    def getItem(self, tableName: str, itemKey: dict, projectionExpression=constants.CHAR__EMPTY, expressionAttributeNames=None, removeAttributeNames=None):
        item = None
        try:
            Logger.log(f'GetItem -> request = tableName={tableName}{constants.CHAR__NEWLINE} itemKey={itemKey}')  
            table = self.client.Table(tableName)
            response = table.get_item(Key=itemKey)

            if response != None and DynamoDBHelper.DDB__ITEM in response:
                if removeAttributeNames != None:
                    for element in removeAttributeNames:
                        if element in response[DynamoDBHelper.DDB__ITEM]:
                            Logger.log(f'GetItem -> Removing element={element}')
                            response[DynamoDBHelper.DDB__ITEM].pop(element)
                item = response[DynamoDBHelper.DDB__ITEM]
                Logger.log(f'GetItem -> item = {item}')
            else:
                Logger.log(f'GetItem -> response = {response}')  
        except ClientError as e:
            Logger.log(f'GetItem -> ERROR = {e}', LogLevel.Exception)      
        return item
   
    
    def putItem(self, tableName: str, item) -> bool:
        try:
            Logger.log(f'PutItem -> request = tableName={tableName}{constants.CHAR__NEWLINE} item={item}')  
            table = self.client.Table(tableName)
            table.put_item(Item=item)            
        except ClientError as e:
            Logger.log(f'PutItem -> ERROR = {e}', LogLevel.Exception)     
            return False
        return True
    

    def deleteItem(self, tableName: str, itemKey: dict) -> bool:
        try:
            Logger.log(f'DeleteItem -> request = tableName={tableName}{constants.CHAR__NEWLINE} itemKey={itemKey}')  
            table = self.client.Table(tableName)
            table.delete_item(Key=itemKey)
        except ClientError as e:
            Logger.log(f'DeleteItem -> ERROR = {e}', LogLevel.Exception)     
            return False
        return True   
    
    
    def updateItem(self, tableName: str, itemKey: dict, attributes: dict) -> dict:
        
        result = {}
               
        # Init update-expression
        updateExpression = DynamoDBHelper.DDB__SET
        lastUpdatedAttribute={
            DynamoDBHelper.DDB_FLD__LAST_UPDATED: str(datetime.now())
        }
        #Add last_updated timestamp
        attributes.update({DynamoDBHelper.DDB_FLD__LAST_UPDATED: str(datetime.now())})
        Logger.log(f'UpdateItem -> request = tableName={tableName}{constants.CHAR__NEWLINE} itemKey={itemKey}{constants.CHAR__NEWLINE} attributes={attributes}')
        
        # Build expression-attribute-names, expression-attribute-values, and the update-expression
        expressionAttributeNames = {}
        expressionAttributevalues = {}
        for key, value in attributes.items():
            updateExpression += f' #{key} = :{key},'  # Notice the "#" to solve issue with reserved keywords
            expressionAttributeNames[f'#{key}'] = key
            expressionAttributevalues[f':{key}'] = value

        # Add counter start and increment attributes
        expressionAttributevalues[':_start'] = 0
        expressionAttributevalues[':_inc'] = 1

        # Finish update-expression with our counter
        updateExpression += " update_counter = if_not_exists(update_counter, :_start) + :_inc"
        Logger.log(f'UpdateItem -> updateExpression={updateExpression}{constants.CHAR__NEWLINE} ExpressionAttributeNames={expressionAttributeNames}{constants.CHAR__NEWLINE} ExpressionAttributeValues={expressionAttributevalues}')
        
        try:
            table = self.client.Table(tableName)
            result = table.update_item(
                Key=itemKey,
                UpdateExpression=updateExpression,
                ExpressionAttributeNames=expressionAttributeNames,
                ExpressionAttributeValues=expressionAttributevalues,
                ReturnValues=DynamoDBHelper.DDB_RETURNVALUES__ALL_OLD
            )
            Logger.log(f'UpdateItem -> result = {result}')          
        except ClientError as e:
            Logger.log(f'UpdateItem -> ERROR = {e}', LogLevel.Exception)     
            return result        
        return result 
    
    def getAllItems(self, tableName: str) -> dict:
        result = {}
        Logger.log(f'GetAllItems -> request = tableName={tableName}')  
        try:
            table = self.client.Table(tableName)
            result = table.scan()
            Logger.log(f'GetAllItems -> result = {result}')
        except ClientError as e:
            Logger.log(f'GetAllItems -> ERROR = {e}', LogLevel.Exception)    
            return result
        return result 
    