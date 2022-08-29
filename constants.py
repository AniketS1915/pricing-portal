# from urllib import response
import json
import sys
import random
import requests
# from main_test import *

url = "https://hooks.slack.com/services/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"
slack_data_login_failed = {
    "username": "Data-Lake-Bot",
    "icon_emoji": ":satellite:",
    "channel" : "#leadgen-alerts",
    "attachments": [
        {
            "color": "#9733EE",
            "fields": [
                {
                    "title": f"New Incoming Message :zap:",
                    "value": "Login Failed",
                    "short": "false",
                }
            ]
        }
    ]
}

slack_data_file_upload_failed = {
    "username": "Data-Lake-Bot",
    "icon_emoji": ":satellite:",
    "channel" : "#leadgen-alerts",
    "attachments": [
        {
            "color": "#9733EE",
            "fields": [
                {
                    "title": f"New Incoming Message :zap:",
                    "value": "file upload failed",
                    "short": "false",
                }
            ]
        }
    ]
}

slack_data_file_upload_successful = {
    "username": "Data-Lake-Bot",
    "icon_emoji": ":satellite:",
    "channel" : "#leadgen-alerts",
    "attachments": [
        {
            "color": "#9733EE",
            "fields": [
                {
                    "title": f"New Incoming Message :zap:",
                    "value": "'File Upload Successful'",
                    "short": "false",
                }
            ]
        }
    ]
}
# if response.status_code != 200:
#     raise Exception(response.status_code, response.text)