# -*- coding: utf-8 -*-
__author__ = 'Mystique'
"""
.. module: Create DynamoDB Table for the given Schema
    :platform: AWS
    :copyright: (c) 2019 Mystique.,
.. moduleauthor:: Mystique
.. contactauthor:: miztiik@github issues
"""
import boto3
import json
import logging


# Initialize Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def set_global_vars():
    """
    Set the Global Variables
    If User provides different values, override defaults

    This function returns the AWS account number

    :return: global_vars
    :rtype: dict
    """
    global_vars = {'status': False, 'error_message': ''}
    try:
        global_vars['Owner']                = "Miztiik"
        global_vars['Environment']          = "Test"
        global_vars['region_name']          = "us-east-1"
        global_vars['tag_key']              = "Valaxy-HelpDesk-Bot"
        global_vars['ddb_table_name']       = "valaxy-butler-queries"
        global_vars['ddb_index_name']       = "valaxy-butler-queries-index"
        global_vars['status']               = True
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
    return global_vars

def create_ddb_table(global_vars):
    """
    Create Dynamo DB Table
    Params
    @table_name: Name of the DDB Table
    @index_name: Name of the Global Secondary Index
    Right now, the reads happen against the GSI and Writes happen again the main table.
    The RCU/WCU provisioning is made with this understanding.
    """
    dynamodb = boto3.resource('dynamodb', region_name = global_vars.get('region_name'))
    response = dynamodb.create_table(TableName = global_vars.get('ddb_table_name'),
                      KeySchema = [{'AttributeName': 'search_query', 'KeyType': 'HASH'}, #Partition key
                                  ],
                      AttributeDefinitions = [{'AttributeType': 'S', 'AttributeName': 'search_query'},
                                              {'AttributeType': 'N', 'AttributeName': 'search_count'}
                                             ],
                      GlobalSecondaryIndexes=[
                                             {
                                                 'IndexName':global_vars.get('ddb_index_name'),
                                                 'KeySchema': [
                                                     {'AttributeName': 'search_count', 'KeyType': 'HASH'}, #Partition key
                                                 ],
                                                 'Projection': { 'ProjectionType': 'ALL' },
                                                 #'Projection': { 'ProjectionType': 'INCLUDE',
                                                 #                 'NonKeyAttributes': ['lead_name',]
                                                 #               },
                                                 'ProvisionedThroughput': {
                                                     'ReadCapacityUnits': 3,
                                                     'WriteCapacityUnits': 1
                                                 }
                                             },
                                            ],
                      ProvisionedThroughput = {
                          'ReadCapacityUnits': 2,
                          'WriteCapacityUnits': 3
                          }
                          )
    return response


def get_table_metadata(global_vars):
    """
    Get some metadata about chosen table.
    """
    dynamodb = boto3.resource('dynamodb', region_name = global_vars['region_name'])
    table = dynamodb.Table( global_vars['ddb_table_name'] )
    table.meta.client.get_waiter('table_exists').wait(TableName= global_vars['ddb_table_name'])
    return {
        'num_items': table.item_count,
        'primary_key_name': table.key_schema[0],
        'status': table.table_status,
        'bytes_size': table.table_size_bytes,
        'global_secondary_indices': table.global_secondary_indexes
    }


def lambda_handler(event, context):
    """
    Entry point for all processing. Load the global_vars

    :return: A dictionary of tagging status
    :rtype: json
    """
    """
    Can Override the global variables using Lambda Environment Parameters
    """
    global_vars = set_global_vars()
    
    resp = {'statusCode': 200, "status": False, "error_message" : '' }

    create_db_resp = create_ddb_table( global_vars )
    logger.info(f"Table status:{create_db_resp.table_status}")
    resp = get_table_metadata( global_vars )
    logger.info(f"-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+")
    logger.info(f"{json.dumps(resp, indent=4)}")
    logger.info(f"-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+")

    return resp

if __name__ == '__main__':
    lambda_handler(None, None)



# References
## [1] [Choosing the Right DynamoDB Partition Key](https://aws.amazon.com/blogs/database/choosing-the-right-dynamodb-partition-key/)
## [2] [Global Secondary Indexes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html)
## [3] [Take Advantage of Sparse Indexes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-indexes-general-sparse-indexes.html#bp-indexes-sparse-examples)
## [4] [Best Practices for Querying and Scanning Data](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-query-scan.html)
## [5] [Querying on Multiple Attributes in Amazon DynamoDB](https://aws.amazon.com/blogs/database/querying-on-multiple-attributes-in-amazon-dynamodb/)

