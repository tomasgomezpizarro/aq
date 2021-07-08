import math
import re


class RequestGenerator:

    def __init__(self):
        self.types = {
            int: 'numberValue',
            float: 'numberValue',
            str: "stringValue",
            bool: 'boolValue'
        }

    def gen_json_update_request(self, rows, sheet_id, a1_position):
        json_rows = list()
        for row in rows:
            json_rows.append(self.gen_json_row(row))

        x, y = self.get_xy_coordinates(a1_position)

        return {
            "updateCells": {
                "rows": json_rows,
                "fields": '*',
                'start': {
                    "sheetId": sheet_id,
                    "rowIndex": y,
                    "columnIndex": x
                }
            }
        }

    @staticmethod
    def _convert_a_to_x(value):
        total = 0
        for i in range(1, len(value) + 1):
            letter = value[-1 * i]
            val = ord(letter) - ord('A')
            if i > 1:
                val = math.pow(26, (i-1)) * (val +1)
            total += val

        total = int(total)
        return total

    @staticmethod
    def _split_a1(value):
        result = re.match('[A-Z]*', value)
        if not result:
            return None
        col_value = result.group()
        row_value = value[result.end():]
        return col_value, row_value

    def get_xy_coordinates(self, value):
        col, row = self._split_a1(value)
        x = self._convert_a_to_x(col)
        y = int(row) - 1
        return x, y

    def gen_json_append_request(self, rows, sheet_id):
        json_rows = list()
        for row in rows:
            json_rows.append(self.gen_json_row(row))

        return {
            "appendCells": {
                "sheetId": sheet_id,
                "rows": json_rows,
                "fields": '*'
            }
        }

    def gen_json_row(self, values):
        json_values = list()
        for value in values:
            json_values.append(self.gen_json_value(value))
        return {'values': json_values}

    def gen_json_value(self, value):
        return {'userEnteredValue': {self.types.get(type(value)): str(value)}}


# TEST HARNESS =================================================================
# run_test = False
#
# if run_test:
#     generator = RequestGenerator()
#
#     result = generator.gen_json_append_request([
#         ['cadillac', 'eldorado', 1973, 30000],
#         ['maserati', 'GT', 80000]],
#         1256842794
#     )
#     print(json.dumps(result, sort_keys=True, indent=4))
#
#     result = generator.gen_json_update_request([
#         ['Peugeot', '308 SW'],
#         ['Peugeot', '406']],
#         1256842794,
#         'A4'
#     )
#     print(json.dumps(result, sort_keys=True, indent=4))
