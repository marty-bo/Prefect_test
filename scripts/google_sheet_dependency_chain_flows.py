

import time
from gspread import Cell
from prefect import flow
import google_sheet_tasks as gst


@flow
def task_1():
    sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
    row = gst.next_available_row(sheet, 1, 2)
    cells = []
    cells.append(Cell(row=row, col=1, vslue=time.strftime('%d/%m/%Y %H:%M:%S')))
    cells.append(Cell(row=row, col=2, vslue="OK"))
    gst.write_in_sheet_with_cells(sheet, cells)


@flow
def task_2():
    sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
    row = gst.next_available_row(sheet, 1, 3)
    cells = []
    cells.append(Cell(row=row, col=1, vslue=time.strftime('%d/%m/%Y %H:%M:%S')))
    cells.append(Cell(row=row, col=3, vslue="OK"))
    gst.write_in_sheet_with_cells(sheet, cells)


@flow
def task_3():
    sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
    row = gst.next_available_row(sheet, 1, 4)
    cells = []
    cells.append(Cell(row=row, col=1, vslue=time.strftime('%d/%m/%Y %H:%M:%S')))
    cells.append(Cell(row=row, col=4, vslue="OK"))
    gst.write_in_sheet_with_cells(sheet, cells)


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