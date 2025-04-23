

import gspread
# from oauth2client.service_account import ServiceAccountCredentials
import time
import random
from prefect import flow, task, get_run_logger
# from prefect.variables import Variable
from google.oauth2.service_account import Credentials
import json
from prefect_gcp import GcpCredentials



@task
def rand_bool(prob:float) -> bool:
    return random.random() < prob

@task
def next_available_row(worksheet:gspread.Worksheet, headers_len:int, col_num:int):
    cells = list(worksheet.col_values(col_num))[headers_len:]
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

    # connect using google credential block
    json_creds = GcpCredentials.load("google-sheet-credentials").service_account_info.get_secret_value()
    creds_dict = json.loads(str(json_creds).replace("'", '"'))
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)

    # connect using prefect variable containing json credential
    # json_creds = str(Variable.get("google_sheet_credentials", "{}")).replace("'", "\"")
    # creds_dict = json.loads(json_creds)
    # creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)


    client = gspread.authorize(creds)
    client.list_spreadsheet_files()
    return client.open("prefect-spreadsheet").sheet1

@task
def write_probs_in_sheet(sheet:gspread.Worksheet, row:int, status1:bool, status2:bool, status3:bool):
    sheet.update(
        values=[[time.strftime('%d/%m/%Y %H:%M:%S'), status1, status2, status3]], 
        range_name=f"A{row}:D{row}")
    sheet.copy_range(f"E{row-1}:G{row-1}", f"E{row}:G{row}")

@task
def testing_prefect_deployment():
    logger = get_run_logger()
    logger.info("Can you see it ?")

@flow
def write_status_in_sheet(prob1:float, prob2:float, prob3:float):
    logger = get_run_logger()
    logger.info(f"Collecting status with probabilities {prob1}, {prob2}, {prob3}")
    sheet = get_worksheet()
    row = next_available_row(sheet, 2, 1)
    logger.info(f"next available row is: {row}")
    status1 = rand_bool(prob1)
    status2 = rand_bool(prob2)
    status3 = rand_bool(prob3)
    logger.info(f"writing status in sheet: {status1}, {status2}, {status3}")
    write_probs_in_sheet(sheet, row, status1, status2, status3)
    testing_prefect_deployment()

@task
def get_status(sheet:gspread.Worksheet, row:int) -> list[str]:
    logger = get_run_logger()
    values = sheet.get(f"B{row}:D{row}")
    logger.info(values)
    return []

@flow
def analye_status():
    logger = get_run_logger()
    logger.info(f"Analysing lasts status")
    sheet = get_worksheet()
    row = next_available_row(sheet, 2, 5) - 1
    logger.info(f"last row is: {row}")
    status = get_status(sheet, row)
    if len(status) != 3:
        logger.error(f"Some status are missing in row {row}")
    if "TRUE" in status:
        logger.error(f"Alert on row {row}")

if __name__ == '__main__':
    write_status_in_sheet()
    analye_status()