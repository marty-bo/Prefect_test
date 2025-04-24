

import time
import asyncio
from gspread import Cell
from prefect import task, flow, get_run_logger
import google_sheet_tasks as gst
from prefect.concurrency.asyncio import concurrency

@task
async def lock_and_work(col:int):
    async with concurrency(names="google_sheet_lock", occupy=1):
        logger = get_run_logger()
        sheet = gst.get_worksheet(gst.DEPENDENCY_CHAIN_SHEET)
        row = gst.next_available_row(sheet, 1, 1)
        logger.info(f"Next available row is: {row}")
        cells = []
        cells.append(Cell(row=row, col=1, value=time.strftime('%d/%m/%Y %H:%M:%S')))
        cells.append(Cell(row=row, col=col, value="OK"))
        gst.write_in_sheet_with_cells(sheet, cells)

    
@flow
async def task_1():
    await lock_and_work(2)


@flow
async def task_2():
    await lock_and_work(3)


@flow
async def task_3():
    await lock_and_work(4)


if __name__ == '__main__':
    asyncio.run(task_1())
    asyncio.run(task_2())
    asyncio.run(task_3())