# -*- coding: utf-8 -*-
__author__ = 'Mystique'
"""
.. module: Get youtube channel metadata 
    :platform: AWS
    :copyright: (c) 2019 Mystique.,
.. moduleauthor:: Mystique
.. contactauthor:: miztiik@github issues
"""

######
# pip3.6 install gdata
# pip3.6 install --upgrade google-api-python-client
# pip3.6 install --upgrade google-auth google-auth-oauthlib google-auth-httplib2
######

import os
import json
import logging
import datetime
from operator import itemgetter
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
        global_vars['tag_key']              = "Valaxy-HelpDesk-Bot"
        global_vars['faq_db_fname']         = "./data/val.json"
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


def slack_msg_builder(msg_data: list):
    """
    Parse message in slack attachment format

    :param slack_webhook_url: The lambda event
    :param type: str
    :param slack_data: A json containing slack performatted text data
    :param type: json

    :return: key_exists Returns True, If key exists, False If Not.
    :rtype: bool
    """
    resp = {'status': False}
    slack_payload = {}
    slack_payload["text"] = ''
    slack_payload["attachments"] = []
    logger.info(msg_data)
    for i in msg_data:
        tmp = {}
        tmp["pretext"]          = f"Check out these demos from #Valaxy Technologies"
        tmp["fallback"]         = "https://www.youtube.com/c/valaxytechnologies"
        # tmp["color"]            = i.get("color")        
        tmp["author_name"]      = "Valaxy Technologies"
        tmp["author_link"]      = "https://www.youtube.com/c/valaxytechnologies"
        tmp["author_icon"]      = "https://avatars1.githubusercontent.com/u/12252564?s=400&u=20375d438d970cb22cc4deda79c1f35c3099f760&v=4"
        tmp["title"]            = f"{i.get('title')}"
        tmp["title_link"]       = f"https://www.youtube.com/watch?v={i.get('vid_id')}"
        tmp["fields"]           = [
                    {
                        "title": "ViewCount",
                        "value": f"`{i.get('view_count')}`",
                        "short": True
                    },
                    {
                        "title": "Popularity",
                        "value": f"`{i.get('popularity')}`",
                        "short": True
                    }
                ]
        tmp["thumb_url"]        = i.get('thumbnails')
        tmp["footer"]           = "Valaxy Butler"
        tmp["footer_icon"]      = "https://avatars.slack-edge.com/2019-03-25/588415136070_fb5a2f732c948b1c83fe_36.png"
        tmp["ts"]               = int( datetime.datetime.now().timestamp() )
        tmp["mrkdwn_in"]        = ["pretext", "text", "fields"]
        slack_payload["attachments"].append(tmp)
    # headers={'Content-Type': 'application/json'} 
    logger.info( json.dumps(slack_payload, indent=4, sort_keys=True) )

    # slack_payload = {'text':json.dumps(i)}
    return slack_payload


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


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response
"""
    "dialogAction": {
    "type": "Close",
    "fulfillmentState": "Fulfilled or Failed",
    "message": {
      "contentType": "PlainText or SSML or CustomPayload",
      "content": "Message to convey to the user. For example, Thanks, your pizza has been ordered."
    },
   "responseCard": {
      "version": integer-value,
      "contentType": "application/vnd.amazonaws.card.generic",
      "genericAttachments": [
          {
             "title":"card-title",
             "subTitle":"card-sub-title",
             "imageUrl":"URL of the image to be shown",
             "attachmentLinkUrl":"URL of the attachment to be associated with the card",
             "buttons":[ 
                 {
                    "text":"button-text",
                    "value":"Value sent to server on button click"
                 }
              ]
           } 
       ] 
     }
  }
"""
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
    logger.info( json.dumps(response, indent=4, sort_keys=True) )
    return response


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


def build_response_card(title, subtitle, options):
    """
    Build a responseCard with a title, subtitle, and an optional set of options which should be displayed as buttons.
    """
    buttons = None
    if options is not None:
        buttons = []
        for i in range(min(5, len(options))):
            buttons.append(options[i])

    return {
        'contentType': 'application/vnd.amazonaws.card.generic',
        'version': 1,
        'genericAttachments': [{
            'title': title,
            'subTitle': subtitle,
            'buttons': buttons
        }]
    }


def build_response_card_slack(options):
    """
    Build a responseCard with a title, subtitle, and an optional set of options which should be displayed as buttons.
    """
    buttons = None
    imageUrl = ''
    if options is not None:
        buttons = []
        imageUrl = options[0].get('thumbnails')
        for i in range(min(5, len(options))):
            buttons.append( { 'text': options[i].get('title'), 'value': f"https://www.youtube.com/watch?v={options[i].get('vid_id')}"})

    return {
        'contentType': 'application/vnd.amazonaws.card.generic',
        'version': 1,
        'genericAttachments': [{
            'title': 'Have a look at these demonstrations',
            'subTitle': 'What will you build today?',
            'attachmentLinkUrl': 'https://www.youtube.com/c/valaxytechnologies',
            'imageUrl': imageUrl,
            'buttons': buttons
        }]
    }


#####################################################################################################
""" --- End of Helper Functions --- """
#####################################################################################################


""" --- Intents --- """
def get_video_id_intent(global_vars: dict, intent_request: dict) -> dict:    
    resp = { "status": False, "error_message": "", 'id_lst': [] }

    haystack = read_from_file( global_vars.get('faq_db_fname') )

    slots = intent_request['currentIntent']['slots']
    output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    # If no slots were matched return
    if not slots['slot_one_svc']:
        resp['error_message'] = f"Your query does not match any AWS Service found. Please try again"
        return resp
    v_ids = []
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
        filter_2 = 3
        s1_v_ids = sorted(v_ids, key=itemgetter('view_count'), reverse=True)[:filter_1]
        s2_v_ids = sorted(s1_v_ids, key=itemgetter('popularity'), reverse=True)[:filter_2]

        # Prepare message for slack
        # msg = slack_msg_builder(s2_v_ids)

        #resp['id_lst'] = s2_v_ids
        resp['status'] = True
    # return resp
    """
    return elicit_slot(
                output_session_attributes,
                intent_request['currentIntent']['name'],
                slots,
                validation_result['violatedSlot'],
                validation_result['message'],
                build_response_card(
                    'Specify {}'.format(validation_result['violatedSlot']),
                    validation_result['message']['content'],
                    build_options(validation_result['violatedSlot'], appointment_type, date, booking_map)
                )
            )
    return close(
        output_session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': json.dumps( resp['id_lst'] )
        }
    )
    """
    return close_w_card(
        output_session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': json.dumps( s2_v_ids )
        },
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
    
    sample_event = {
          "messageVersion": "1.0",
          "invocationSource": "FulfillmentCodeHook",
          "userId": "recpfpf5hkvnmn3fx0ew9gtrga03q8ek",
          "sessionAttributes": {},
          "requestAttributes": None,
          "bot": {
            "name": "valaxy_helpdesk_bot",
            "alias": "$LATEST",
            "version": "$LATEST"
          },
          "outputDialogMode": "Text",
          "currentIntent": {
            "name": "get_video_id_intent",
            "slots": {
              "slot_one_svc": "EC2"
            },
            "slotDetails": {
              "slot_one_svc": {
                "resolutions": [
                  {
                    "value": "EC2"
                  }
                ],
                "originalValue": "EC2"
              }
            },
            "confirmationStatus": "None",
            "sourceLexNLUIntentInterpretation": None
          },
          "inputTranscript": "How to launch an EC2"
        }

    resp = {'statusCode': 200, "status": False, "error_message" : '' , 'body': json.dumps(event) }

    resp_chk(global_vars.get('status'), global_vars.get('error_message'))

    faq_db = read_from_file( global_vars.get('faq_db_fname') )

    # event['currentIntent']['name']
    needle = None
    if 'currentIntent' in event and 'slots' in event['currentIntent']:
        needle = event['currentIntent']['slots']
        
    # v_ids = get_video_id_intent(needle, faq_db)
    
    # resp['v_ids'] = v_ids.get('id_lst')

    logger.debug(f"event.bot.name={event['bot']['name']}")

    return dispatch(global_vars, event)

if __name__ == '__main__':
    lambda_handler(None, None)
