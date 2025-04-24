

import gspread
from prefect import task
from google.oauth2.service_account import Credentials
import json
from prefect_gcp import GcpCredentials


SHEET1_SHEET = "0"
DEPENDENCY_CHAIN_SHEET = "2123097791"

@task(tags={"google-sheet"})
def next_available_row(worksheet:gspread.Worksheet, headers_len:int, col_num:int):
    cells = list(worksheet.col_values(col_num))[headers_len:]
    if '' in cells:
        return cells.index('') + 1 + headers_len
    return len(cells) + 1 + headers_len


@task(tags={"google-sheet"})
def get_worksheet(sheet_id:str) -> gspread.Worksheet:
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
    return client.open("prefect-spreadsheet").get_worksheet_by_id(sheet_id)


@task(tags={"google-sheet"})
def read_in_sheet(sheet:gspread.Worksheet, range:str) -> list[list[str]]:
    return sheet.get(range_name=range)


@task(tags={"google-sheet"})
def write_in_sheet(sheet:gspread.Worksheet, range:str, values:list[list]):
    sheet.update(
        values=values,
        range_name=range
    )


@task(tags={"google-sheet"})
def copy_in_sheet(sheet:gspread.Worksheet, from_range:str, to_range:str):
    sheet.copy_range(source=from_range, dest=to_range)