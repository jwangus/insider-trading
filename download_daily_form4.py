import logging
import os
from _secrets import LOG_FOLDER
from secfilings import download_form4, get_report_date

if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(LOG_FOLDER, 'download_daily_form4.log'),
                        format='%(asctime)s - %(message)s', level=logging.INFO)
    
    #export SEC_REPORT_DATE=2022-01-01 && python download_daily_form4.py
    report_date = get_report_date()
    logging.info(f"Downloading file for date: {report_date}")

    try:
        download_form4(report_date)
    except Exception as e:
        logging.exception(str(e))
    else:
        logging.info("Download Completed.")
