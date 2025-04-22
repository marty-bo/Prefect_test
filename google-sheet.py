

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import random
from prefect import flow, task, get_run_logger
from prefect.variables import Variable
from google.oauth2.service_account import Credentials
import json

json_creds = Variable.get("google_sheet_credentials", "{}")
print(json_creds)
print(type(json_creds))
print(str(json_creds))
#exit()

@task
def rand_bool(prob:float) -> bool:
    return random.random() < prob

@task
def next_available_row(worksheet:gspread.Worksheet, headers_len:int):
    cells = list(worksheet.col_values(1))[headers_len:]
    if '' in cells:
        return cells.index('') + 1 + headers_len
    return len(cells) + 1 + headers_len

@task
def get_worksheet() -> gspread.Worksheet:
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    # connect using local json credential file
    # creds = ServiceAccountCredentials.from_json_keyfile_name("creds/prefect-test.json", scopes)

    # connect using prefect variable containing json credential
    json_creds = str(Variable.get("google_sheet_credentials", "{}")).replace("'", "\"")
    creds_dict = json.loads(json_creds)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)


    client = gspread.authorize(creds)
    client.list_spreadsheet_files()
    return client.open("prefect-spreadsheet").sheet1

@task
def write_in_sheet(sheet:gspread.Worksheet, row:int, status1:float, status2:float, status3:float):
    sheet.update(
        values=[[time.strftime('%d/%m/%Y %H:%M:%S'), status1, status2, status3]], 
        range_name=f"A{row}:D{row}")
    sheet.copy_range(f"E{row-1}:G{row-1}", f"E{row}:G{row}")

@flow
def write_status_in_sheet():
    sheet = get_worksheet()
    row = next_available_row(sheet, 2)
    status1 = rand_bool(0.5)
    status2 = rand_bool(0.2)
    status3 = rand_bool(0.05)
    write_in_sheet(sheet, row, status1, status2, status3)
    

if __name__ == '__main__':
    write_status_in_sheet()