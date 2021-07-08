import traceback
from multiprocessing import Process
import gspread
import gspread.utils
import gspread.utils
from aq_lib.utils.logger import logger
from gspread import WorksheetNotFound


class SheetLogger(Process):
    # scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    #
    # def __init__(self,google_json=None, sheet_id=None,queue=None):
    #     self.logger_queue = queue
    #     google_json = google_json.replace("\n","\\n")
    #     google_dict = json.loads(google_json)
    #     self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(google_dict, self.scope)
    #     self.client = gspread.authorize(self.credentials)
    #     if sheet_id is not None:
    #         self.spreadsheet = self.client.open(sheet_id)

    def __init__(self, client=None, spreadsheet=None, queue=None):
        super(SheetLogger, self).__init__()
        self.client = client
        self.spreadsheet = spreadsheet
        self.logger_queue = queue

    #def open_spreadsheet(self,sheet_id):
     #   self.spreadsheet = self.client.open(sheet_id)

    def run(self):
        while True:
            # item =  self.logger_queue.get()
            # self.process_item(item)
            pass

    def process_item(self, item):
        try:
            self.client.login()  # TODO revisar si es neceario
            try:
                sheet = self.spreadsheet.worksheet(item.worksheet_name)
            except WorksheetNotFound:
                sheet = self.spreadsheet.add_worksheet(title=item.worksheet_name, rows=1, cols=100)

            key,values = self.recursive_items(item.values)

            if item.cell is not None:
                cells = self.get_cell(sheet, item.cell, len(item.values))
                for i, cell in enumerate(cells):
                    cell.value = values[i]
                sheet.update_cells(cells)
            else:
                row_count = sheet.row_count
                sheet.insert_row(values, row_count)
        except Exception:
            logger.warning("Exception loggin to sheets")
            logger.warning(traceback.format_exc())

    def get_cell(self, sheet, cell, qty):
        coords = gspread.utils.a1_to_rowcol(cell)
        range = cell + ":" + gspread.utils.rowcol_to_a1(coords[0], coords[1] + qty-1)
        return sheet.range(range)

    @staticmethod
    def recursive_items(dictionary):
        keys = []
        values = []
        for key, value in dictionary.items():
            if type(value) is dict:
                for k1, v1 in value.items():
                    keys.append(k1)
                    values.append(v1)
            else:
                keys.append(key)
                values.append(value)
        return keys, values



