

import time
from prefect import flow
import google_sheet_tasks as gst


@flow
def task_1():
    sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
    row = gst.next_available_row(sheet, 1, 2)
    gst.write_in_sheet(sheet, f"A{row},B{row}", [[time.strftime('%d/%m/%Y %H:%M:%S'), "OK"]])


@flow
def task_2():
    sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
    row = gst.next_available_row(sheet, 1, 3)
    gst.write_in_sheet(sheet, f"A{row},C{row}", [[time.strftime('%d/%m/%Y %H:%M:%S'), "OK"]])


@flow
def task_3():
    sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
    row = gst.next_available_row(sheet, 1, 4)
    gst.write_in_sheet(sheet, f"A{row},D{row}", [[time.strftime('%d/%m/%Y %H:%M:%S'), "OK"]])


# @flow
# def task_4():
#     sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
#     row = gst.next_available_row(sheet, 1, 5)
#     gst.write_in_sheet(sheet, f"A{row},E{row}", [[time.strftime('%d/%m/%Y %H:%M:%S'), "OK"]])



# @flow
# def task_5():
#     sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
#     row = gst.next_available_row(sheet, 1, 6)
#     gst.write_in_sheet(sheet, f"A{row},F{row}", [[time.strftime('%d/%m/%Y %H:%M:%S'), "OK"]])


if __name__ == '__main__':
    task_1()