

import random
import time
from prefect import flow, task, get_run_logger
import google_sheet_tasks as gst


@task
def rand_bool(prob:float) -> bool:
    return random.random() < prob

@flow
def write_status_in_sheet(prob1:float, prob2:float, prob3:float):
    logger = get_run_logger()
    logger.info(f"Collecting status with probabilities {prob1}, {prob2}, {prob3}")
    sheet = gst.get_worksheet(gst.SHEET1_SHEET)
    row = gst.next_available_row(sheet, 2, 1)
    logger.info(f"next available row is: {row}")
    status1 = rand_bool(prob1)
    status2 = rand_bool(prob2)
    status3 = "" if rand_bool(prob3) else "OK"
    logger.info(f"writing status in sheet: '{status1}', '{status2}', '{status3}'")
    gst.write_in_sheet(sheet, f"A{row}:D{row}", [[time.strftime('%d/%m/%Y %H:%M:%S'), status1, status2, status3]])


@flow
def analye_status():
    logger = get_run_logger()
    logger.info("Analysing lasts status")
    sheet = gst.get_worksheet(gst.SHEET1_SHEET)
    row = gst.next_available_row(sheet, 2, 1) - 1 # last row
    if row <= 2:
        logger.error("No row to analyse")
        return
    logger.info(f"last row is: {row}")
    analyse = gst.read_in_sheet(sheet, f"E{row}")[0]
    if len(analyse) > 0:
        logger.error(f"Analyse of row {row} is already done")
        return
    status = gst.read_in_sheet(sheet, f"B{row}:D{row}")[0]
    analyses = []
    if len(status) != 3:
        logger.error(f"Some status are missing in row {row}")
        analyses.append("Missing status")
    if "FALSE" in status:
        logger.error(f"Alert on row {row}")
        analyses.append("Alert")
    if len(analyses) == 0:
        logger.info(f"Row {row} is OK")
        analyses.append("OK")
    gst.write_in_sheet(sheet, f"E{row}", [["; ".join(analyses)]])

if __name__ == '__main__':
    write_status_in_sheet()
    analye_status()