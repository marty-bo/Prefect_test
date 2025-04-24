

import time
from gspread import Cell
from prefect import flow
import google_sheet_tasks as gst


@flow
def task_1():
    sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
    row = gst.next_available_row(sheet, 1, 1)
    cells = []
    cells.append(Cell(row=row, col=1, value=time.strftime('%d/%m/%Y %H:%M:%S')))
    cells.append(Cell(row=row, col=2, value="OK"))
    gst.write_in_sheet_with_cells(sheet, cells)


@flow
def task_2():
    sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
    row = gst.next_available_row(sheet, 1, 1)
    cells = []
    cells.append(Cell(row=row, col=1, value=time.strftime('%d/%m/%Y %H:%M:%S')))
    cells.append(Cell(row=row, col=3, value="OK"))
    gst.write_in_sheet_with_cells(sheet, cells)


@flow
def task_3():
    sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
    row = gst.next_available_row(sheet, 1, 1)
    cells = []
    cells.append(Cell(row=row, col=1, value=time.strftime('%d/%m/%Y %H:%M:%S')))
    cells.append(Cell(row=row, col=4, value="OK"))
    gst.write_in_sheet_with_cells(sheet, cells)


if __name__ == '__main__':
    task_1()
    task_2()
    task_3()