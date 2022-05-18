from secfilings import generate_daily_summary_report, report_date_range, generate_daily_summary_report_data, process_download
from secfilings import previous_weekday
from _secrets import LOG_FOLDER
from _secrets import REPORTS_FOLDER
import logging
import os
import pandas as pd

if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(LOG_FOLDER, 'create_daily_insider_trade_summary.log'),
                        format='%(asctime)s %(name)s %(levelname)s - %(message)s', level=logging.INFO)
    logging.getLogger().setLevel(logging.WARN)

    log = logging.getLogger("insider_tx_summary")
    log.setLevel(logging.INFO)

    report_date = previous_weekday()

    for report_date in report_date_range():
        if report_date.weekday() > 4:
            logging.info(f"Skipping weekend. Date: {report_date}")
            continue
        log.info(
            f"generate daily inside trading summray file for date: {report_date}")

        try:
            log.info("1. extracting report data downloaded")
            if process_download(report_date) == 0:
                raise ValueError("Could not find downloaded forms!")
            log.info("2. createing report")
            rep_data = generate_daily_summary_report_data(report_date)
            df_by_ticker, df_by_insider, df_raw, df_insider_raw = generate_daily_summary_report(
                rep_data)

            filename = os.path.join(
                REPORTS_FOLDER, f"insider_reports_{report_date}")

            log.info(f"3. saving html files: {filename}")
            with open(filename+"_by_ticker.html", "w") as f:
                f.write(df_by_ticker.to_html(escape=False))
            with open(filename+".html", "w") as f:
                f.write(df_by_insider.to_html(escape=False))
            # add report date and save to csv
            df_insider_raw['report_date'] = report_date
            df_insider_raw.to_csv(filename+".csv", index=False)
            df_raw['report_date'] = report_date
            df_raw.to_csv(filename+"_raw.csv", index=False)

        except Exception as e:
            log.exception(str(e))
        else:
            log.info("generate daily inside trading summray file completed.")
