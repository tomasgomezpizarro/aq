from aq_lib.utils.logger import logger
from gspread import WorksheetNotFound
from gspread.exceptions import APIError
import gspread


class SheetReader:

    def __init__(self, credentials, spreadsheet_id):
        client = gspread.authorize(credentials)
        try:
            self.spreadsheet = client.open_by_key(spreadsheet_id)
        except Exception as e:
            logger.warning("Spreadsheet not found")

    def row_values(self, sheet_name=None, row=None):
        if sheet_name is not None and row is not None:
            try:
                sheet = self.spreadsheet.worksheet(sheet_name)
                return sheet.row_values(row)
            except WorksheetNotFound as e:
                logger.warning("Intentando abrir una hoja que no existe")
            except APIError as e:
                logger.warning("Error intentando leer un spreadsheet")
        else:
            logger.warning("Sheet name or row not specified")

    def col_values(self, sheet_name = None, col=None):
        if sheet_name is not None and col is not None:
            try:
                sheet = self.spreadsheet.worksheet(sheet_name)
                return sheet.col_values(col)
            except WorksheetNotFound as e:
                logger.warning("Intentando abrir una hoja que no existe")
            except APIError as e:
                logger.warning("Error intentando leer un spreadsheet")
            except ValueError as e:
                logger.warning("Parámetro col debe ser int")
        else:
            logger.warning("Sheet name or row not specified")

    def get_all_values(self, sheet_name):
        if sheet_name is not None:
            try:
                sheet = self.spreadsheet.worksheet(sheet_name)
                return sheet.get_all_values()
            except WorksheetNotFound:
                logger.warning("Intentando abrir una hoja que no existe")
        else:
            logger.warning("Sheet name not specified")

    def get_all_records(self, sheet_name):
        if sheet_name is not None:
            try:
                logger.debug("Querying all records for %s" %sheet_name)
                sheet = self.spreadsheet.worksheet(sheet_name)
                return sheet.get_all_records()
            except WorksheetNotFound:
                logger.warning("Intentando abrir una hoja que no existe")
        else:
            logger.warning("Sheet name not specified")
