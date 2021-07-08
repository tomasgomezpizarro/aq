import json
from oauth2client.service_account import ServiceAccountCredentials
from arquants.sheets.gsheet_reader import SheetReader
from arquants.sheets.queue_reader import QueueReader
from arquants.sheets.accumulator import Accumulator
from arquants.sheets.batch_logger import BatchLogger


class SheetManager:

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    def __init__(self, google_json=None, spreadsheet_id=None, queue=None):
        self.logger_queue = queue
        self.spreadsheet_id = spreadsheet_id
        print(google_json)
        google_dict = json.loads(google_json)
        self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(google_dict, self.scope)

        self.batch_logger = BatchLogger(self.credentials, self.spreadsheet_id)
        self.accumulator = Accumulator(self.batch_logger.process_items)
        self.queue_reader = QueueReader(queue=self.logger_queue, processor=self.accumulator)

        self.queue_reader.start()
        self.accumulator.start()
        self.gsheet_reader = SheetReader(self.credentials, self.spreadsheet_id)


#--------------------------Test Code--------------------------
# class SheetItemMock:
#
#     def __init__(self,worksheet_name=None,cell=None,values=[]):
#         self.worksheet_name = worksheet_name
#         self.cell = cell
#         self.values = values
#
# google_json = '{"type": "service_account", "project_id": "quantsheets","private_key_id": "81c822a44a4eaa13bebb1cce0f5616d78e5cbdee",' \
#         '"private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC4H3miSsZgkFgZ\\n0n+mN45gyky8QaJoaZg2B/UwDxjcdY3+f9EyYqWCdnmCT0NL2yGHJ+jUeH3P48CN\\n/n87Xk24LLbBaDawFvquewlyYBIiPI03fy1U04SvkkJ+P/n1eXeFOWrJc6RYF7eH\\nutJ42nD8WfhKdgHYA3O3ckUO55LENztJQefongG3la4jyo9HTGCO/i5k6WqSqzTF\\nY9yDhNVTGgFwEmGDXAZXxh0J8AJzmMpnBtJ9hR6NQZDKQrORHcFdoQBkMe/RLlOm\\n/8n37ZebEqHkXftyRUGpSJbItswBxEkSv53SnCFnFEpd5oSfzthJaIwZtRV9Bf6W\\nQ83VLnAlAgMBAAECggEACTUuapwQMHnkz1fG9puZJRnWKA+lN6ZY4bzYpBBhNycx\\nfxcfxwQGuw/siC8pZ6SmrYEPWaQemz6hyB1rJJ9/m1SwcrAg6gH1g95sd286q5fO\\ncnEg+yNqeN2Y3IJGYGJfGdxoTM5EcKZbpoVEgMg1yxzpt+Q6Yosw+9fSVco6vVFg\\nVJ4rYeleP9Thh8PUVlN9thpNzBvmm1Reb0cFrr3va519KZyNUOmtoSSEngCEU7sA\\nsetJPpk3uQ5jkfBDA2/xfjOhxEkwmmIkKtnIR5q20UM3i4yDU/nY5BWBhAwivatR\\n8VRZsx7drS089dqxIKBZLgne3ir+PyZwbV5oSaKXOQKBgQDq6S8rDW6cqtNdxmaC\\nknlDz3hEyERmntFbUI2pwkojDStV36o1lr9sVq2ZSN+wqYaUuqR2c0QGhVYC1TE3\\ngOp0A9DW7W9sJqz+ZVLrcVYuRD6ZveizEoHFze9JiZT5TsE3/VITv9/BfCfcE5Yt\\nbz/INtn1xg2rOQIjeOimXJX0zQKBgQDIpw/7+4XTACJQ1chjKYxBvt1OZof+XoMy\\nrfH54gUX8Vvkaf4Qe/h5u5DqFxDT2YYz6LMqNUvGwVu3sddrfQSXVFlC0tlHdPEG\\n3EdtI1H76juTV29tc4gGt14ZBOpDnCPy7Y5D6bJfdW0iA86xIobjrPUj7Lp8uJfb\\nGofuhNKouQKBgDDToyA5vIIH58MYF/qP43C60O7LCZyi6jUmuytL3QkIFbfVs5VL\\n7iHgqliwEv6vXe/QE5sjPkJ0uHoCQiadPx46JNBnrb83EsIV4XRarGUVfkWKebGu\\n6RZqRZBtbm+bdQHkP4knWqTm34oY8CAlfYZqEEfLkM/EG7Ovz/u0Rt8JAoGATsbY\\nTbPoTHjnABmOvO/Y8w7+UAONLN7qX4FPWS3VlhraWwkKCGrDmPd844r+vk3OlJ6t\\niMq332aWnb4itz60CL3C9atWRumwn2LoX/7X9zF1Blnzk2MiapCfTu9REg5BRuTh\\nT+R2dWfi2tHn2j+V+dkzvVD5vnGynQEDkS9wjXECgYEA45OQDUYxTEuVr4ZUKVvU\\nYC2fIKeetiqo04yia48I6OZ8J7lNIxPL4l2YwGN95I1iSxwbSbUXk7E0227ygZST\\nWsOLOFktfnbSKBulSgPRiqXyI469M1Rc6qccoprfiy4Adw3Cd1pwX5KOpZEhzVVT\\nPS9EgZJ2ppKPgFeZ+X6t1kw=\\n-----END PRIVATE KEY-----\\n",' \
#         '"client_email": "quantsheet@quantsheets.iam.gserviceaccount.com",' \
#         '"client_id": "113805095232193113831",' \
#         '"auth_uri": "https://accounts.google.com/o/oauth2/auth",' \
#         '"token_uri": "https://oauth2.googleapis.com/token",' \
#         '"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",' \
#         '"client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/quantsheet%40quantsheets.iam.gserviceaccount.com"}'
#
# sl = SheetManager(spreadsheet_id='Test', google_json=google_json)
#
# a=dict()
# a['columna1'] = 'raul'
# a['columna2'] = 'pepito'
# a['columna3'] = 'cachito'
# item = SheetItemMock(worksheet_name='asdasd', cell='B2')
# item.values = a
#
# sl.accumulator.process_item(item)
#
# result = sl.gsheet_reader.get_all_records(sheet_name='asdasd')
# print(result)
