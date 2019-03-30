# -*- coding: utf-8 -*-
__author__ = 'Mystique'
"""
.. module: Get youtube channel metadata 
    :platform: AWS
    :copyright: (c) 2019 Mystique.,
.. moduleauthor:: Mystique
.. contactauthor:: miztiik@github issues
"""

import os
import json
import logging
import datetime
from operator import itemgetter
import boto3
from boto3.dynamodb.conditions import Key, Attr
import asyncio

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
        global_vars['faq_db_fname']         = "./data/val.json"
        global_vars['ddb_table_name']       = "valaxy-butler-queries"
        global_vars['update_ddb']           = True
        global_vars['status']               = True
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
    return global_vars


def resp_chk(resp_status: bool, resp_err: str):
    # Check and continue if no errors
    if not resp_status:
        logger.error(f"ERROR: {resp_err}")


def read_from_file(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
    return data


def safe_div(n, d):
    return n / d if d else 0

#####################################################################################################
""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """
#####################################################################################################


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def elicit_slot_w_response(session_attributes, intent_name, slots, slot_to_elicit, message, response_card):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message,
            'responseCard': response_card
        }
    }


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


def confirm_intent(session_attributes, intent_name, slots, message, response_card):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message,
            'responseCard': response_card
        }
    }


def check_item_exists(region_name: str, table_name: str, needle: str) -> bool:
    """
    Check if the given dynamodb item exists.
    Query with limit of 1 is performed.
    """
    resp = { 'item_exists':False }
    if not needle: needle = 'auto'
    client = boto3.client('dynamodb', region_name = region_name)
    try:
        # resp['Items'] = table.query( KeyConditionExpression = Key('search_query').eq( str(needle).lower() ) )
        r1 = client.query(TableName = table_name,
                            KeyConditionExpression='search_query = :var1',
                            ExpressionAttributeValues={
                                ":var1":{"S": needle.lower()}
                            },
                            ProjectionExpression = "search_query, #ui, utterances",
                            ExpressionAttributeNames = {'#ui': 'user_ids'}
                            )
        if ( r1.get('Count') == 1 or r1.get('Count')>1 ) and len(r1.get('Items')) == 1:
            resp['Items'] = r1.get('Items')
            resp['item_exists'] =  True
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
    return resp


async def create_ddb_item(region_name: str, table_name: str, item: dict):
    """
    Create DDB Item
    """
    dynamodb = boto3.resource('dynamodb', region_name = region_name)
    table = dynamodb.Table(table_name)
    try:
        response = table.put_item(
           Item={
                'search_query': str( item.get('search_query') ),
                'search_count': 1,
                'created_on' : str(datetime.datetime.now()),
                'user_ids': [item.get('user_id')],
                'utterances': [item.get('utterance')]
            }
        )
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")


async def update_ddb_item(region_name: str, table_name: str, item: dict):
    """
    Helper function to Insert / Update item in table
    Add email_sent attribute and set to true
    REMOVE next_lead attribute
    """
    dynamodb = boto3.resource('dynamodb', region_name = region_name)
    table = dynamodb.Table(table_name)
    try:
        u_ex = f'SET search_count= search_count + :incr, last_searched= :var2'
        ex_val = {
                    ':incr' : 1,
                    ':var2' :str(datetime.datetime.now())
                }
        if item.get('user_id'):
            u_ex+=f', user_ids = list_append(user_ids, :var3)'
            ex_val[':var3'] = [str( item.get('user_id') ) ]
        if item.get('utterance'):
            u_ex+=f', utterances = list_append(utterances, :var4)'
            ex_val[':var4'] = [str( item.get('utterance') ) ]
        response = table.update_item(TableName = table_name,
                                        Key={'search_query':str( item.get('search_query') ) },
                                        # UpdateExpression='SET email_sent= :var1 REMOVE next_lead',
                                        UpdateExpression = u_ex,
                                        ExpressionAttributeValues = ex_val
                                    )
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")


def build_response_card_slack(options):
    """
    Build a responseCard with a title, subtitle, and an optional set of options which should be displayed as buttons.
    """
    cards = []
    if options is not None:

        # imageUrl = options[0].get('thumbnails')
        for i in range(min(5, len(options))):
            t = {}
            t['title'] = f"{options[i].get('title')[:75]}..."
            t['subTitle'] = f"*ViewCount*: *`{options[i].get('view_count')}`* *Popularity*: *`{options[i].get('popularity')}`*"
            t['attachmentLinkUrl'] = f"https://www.youtube.com/watch?v={options[i].get('vid_id')}"
            t['imageUrl'] = options[i].get('thumbnails')
            cards.append(t)
    return {
        'contentType': 'application/vnd.amazonaws.card.generic',
        'version': 1,
        'genericAttachments': cards
        }


def close_w_card(session_attributes, fulfillment_state, message, options):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message,
            "responseCard": build_response_card_slack(options)
        }
    }
    logger.debug( json.dumps(response, indent=4, sort_keys=True) )
    return response


def close(session_attributes, fulfillment_state, message_content):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message_content
        }
    }
    return response
#####################################################################################################
""" --- End of Helper Functions --- """
#####################################################################################################


""" --- Intents --- """
def get_video_id_intent(global_vars: dict, intent_request: dict) -> dict:    
    resp = { "status": False, "error_message": "", 'id_lst': [] }

    # Slots are dictionary
    slots = intent_request['currentIntent']['slots']
    output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    # If no slots were matched return
    if not slots['slot_one_svc']:
        return elicit_slot(output_session_attributes, 
                            intent_request['currentIntent']['name'],
                            slots,
                            'slot_one_svc',
                            f"Your query does not match any AWS Service found. Please try again"
                            )
    # Update Dynamo only if user wants to.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    if global_vars.get('update_ddb'):
        # Insert / Update Dynamodb about search query
        item = { 'search_query': slots['slot_one_svc'].lower(),
                 'utterance': intent_request.get('inputTranscript'),
                 'user_id' : intent_request.get('userId')
             }
        i_data = check_item_exists( global_vars.get('region_name'), global_vars.get('ddb_table_name'), slots['slot_one_svc'] )
        print(i_data)
        if not i_data.get('item_exists'):
            loop.run_until_complete( create_ddb_item(global_vars.get('region_name'), global_vars.get('ddb_table_name'), item) )
        else:
            # To avoid adding the same user_ids & utterances again, check
            for i in i_data.get('Items')[0].get('utterances')['L']:
                if item.get('utterance') in i['S']:
                    item.pop('utterance', None)
                    break
            for i in i_data.get('Items')[0].get('user_ids')['L']:
                if item.get('user_id') in i['S']:
                    item.pop('user_id', None)
                    break
            loop.run_until_complete( update_ddb_item(global_vars.get('region_name'), global_vars.get('ddb_table_name'), item) )
        """
        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
                if flower_type is not None:
                    output_session_attributes['Price'] = len(flower_type) * 5  # Elegant pricing model
        """
    loop.close()
    
    # Begin searching for the search_query(needle) in the haystack
    v_ids = []
    haystack = read_from_file( global_vars.get('faq_db_fname') )
    for i in haystack['vids']:
        if slots['slot_one_svc'].lower() in i[0]['title'].lower():
            # resp['id_lst'].append( {'title': i[0]['title'] , 'id': i[0]['vid_id'] } )
            num = 0
            denom = 0
            if 'likeCount' in i[0]['statistics'] and not None:
                num = int( i[0]['statistics'].get('likeCount') )
            if 'dislikeCount' in i[0]['statistics'] and not None:
                denom = int( i[0]['statistics'].get('dislikeCount') )
            popularity = int( safe_div(num, (num+denom)) * 100 )
            v_ids.append( { 'title': i[0]['title'] , 
                            'vid_id': i[0]['vid_id'], 
                            'view_count' : int( i[0]['statistics'].get('viewCount') ), 
                            'popularity': popularity,
                            'thumbnails' : i[0].get('thumbnails')
                            }
                        )
            # print("{0:.0%}".format(1./3))

    if v_ids:
        # Sorting the videos to find the most suitable one
        """
        Sort Criteria 1: ViewCount - Top 10 filtered
        Sort Criteria 2: Popularity - Top 3 Returned
            Popularity: ( LikeCount / TotalFeedback )
        """
        filter_1 = 10
        filter_2 = 5
        s1_v_ids = sorted(v_ids, key=itemgetter('view_count'), reverse=True)[:filter_1]
        s2_v_ids = sorted(s1_v_ids, key=itemgetter('popularity'), reverse=True)[:filter_2]

    return close_w_card(
        output_session_attributes,
        'Fulfilled',
        {'contentType': 'PlainText', 'content': 'Have a look at these demonstrations from #Valaxy'},
        s2_v_ids
    )


def dispatch(global_vars, intent_request):
    """
    Called when the user specifies an intent for this bot.
    """
    logger.debug(f"dispatch userId={intent_request['userId']}, intentName={intent_request['currentIntent']['name']}")

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'get_video_id_intent':
        return get_video_id_intent(global_vars, intent_request)

    raise Exception(f"Intent with name {intent_name} not supported")

#####################################################################################################
""" --- End of Intents --- """
#####################################################################################################


""" --- Main handler --- """


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

    resp_chk(global_vars.get('status'), global_vars.get('error_message'))  

    # logger.debug(f"event.bot.name={event['bot']['name']}")

    return dispatch(global_vars, event)

if __name__ == '__main__':
    lambda_handler(None, None)
