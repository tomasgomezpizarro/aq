from googleapiclient.discovery import build
import traceback
import json
from arquants.sheets.json_generator import RequestGenerator


class BatchLogger:

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    def __init__(self, creds, spreadsheet_id):
        self.api = build('sheets', 'v4', credentials=creds).spreadsheets()
        self.generator = RequestGenerator()
        self.spreadsheet_id = spreadsheet_id
        self.sheet_names = dict()
        self._load_sheet_ids()

    def process_items(self, items):
        print('procesando item')
        requests = list()

        for item in items:
            sheet_id = self.get_or_create_sheet(item.worksheet_name)
            if not item.cell:
                request = self.generator.gen_json_append_request(item.values, sheet_id)
            else:
                request = self.generator.gen_json_update_request(item.values, sheet_id, item.cell)
            requests.append(request)

        self._batch_execute(requests)

    def _batch_execute(self, requests):
        try:
            request_body = {'requests': requests}
            response = self.api.batchUpdate(spreadsheetId=self.spreadsheet_id, body=request_body).execute()
            return response
        except Exception as e:
            log(traceback.format_exc())

    def _load_sheet_ids(self):
        spreadsheet = self.api.get(spreadsheetId=self.spreadsheet_id, fields='sheets(data/rowData/values/userEnteredValue,properties(index,sheetId,title))').execute()
        for sheet in spreadsheet['sheets']:
            sheet_name = sheet['properties']['title']
            sheet_id = sheet['properties']['sheetId']
            self.sheet_names[sheet_name] = sheet_id
        print('Loaded sheets : ', self.sheet_names)
        print('spreadsheet info : ', spreadsheet)
        print(json.dumps(spreadsheet, sort_keys=True, indent=4))

    def get_or_create_sheet(self, name):
        if not self.sheet_names.get(name):
            log('sheet {} does not exist, will create it'.format(name))
            self.sheet_names[name] = self.create_sheet(name)
        return self.sheet_names[name]

    def create_sheet(self, sheet_name):
        try:
            request_body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_name,
                        }
                    }
                }]
            }
            response = self.api.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=request_body
            ).execute()
            sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
            return sheet_id

        except Exception as e:
            log(traceback.format_exc())

    # Draft implementation =========================================================================
    # ==============================================================================================

    def append(self, sheet_name, values):
        self.ensure_sheet_exists(sheet_name)

        body = {
            'values': values
        }
        result = self.api.values().append(spreadsheetId=self.spreadsheet_id, range=sheet_name,
                                          valueInputOption='USER_ENTERED', body=body).execute()
        print(result)

    def update(self, update_range, values):
        sheet_name = update_range.split(',')[0]
        log('Updating sheet {}'.format(sheet_name))

        self.ensure_sheet_exists(sheet_name)

        body = {
            'values': values
        }
        result = self.api.values().update(spreadsheetId=self.spreadsheet_id, range=update_range,
                                          valueInputOption='USER_ENTERED', body=body).execute()
        print(result)

    def get(self, get_range):
        result = self.api.values().get(spreadsheetId=self.spreadsheet_id, range=get_range).execute()
        print(result)

def log(message):
    print(message)

# ==================================================================================================
# TEST HARNESS
# ==================================================================================================
# class MockItem:

#    def __init__(self,worksheet_name=None,cell=None,values=[]):
#        self.worksheet_name = worksheet_name
#        self.cell = cell
#        self.values = values

# class Glogger_tester:
#    def run(self):

#        creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", BatchLogger.scope)
#        sid = '1k7Wmi-Wsb-LPdEBhGOI6uiztwTkcsPPBfiUFmCyNRm0'
#        glogger = BatchLogger(creds, sid)

#        append_values = [
#            ['cadillac', 'eldorado', 1973, 30000],
#            ['maserati', 'GT', 80000]]
#         append_item = MockItem()
#         append_item.values = append_values
#         append_item.worksheet_name = 'Cars'
#
#         update_values = [
#             ['Peugeot', '308 SW'],
#             ['Peugeot', '406']]
#         update_item = MockItem()
#         update_item.values = update_values
#         update_item.cell = 'A2'
#         update_item.worksheet_name = 'Cars'

        # glogger.process_items([append_item, update_item])

# glogger_tester = Glogger_tester()
# glogger_tester.run()
