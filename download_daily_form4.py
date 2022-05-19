from datetime import date
import logging
import os
from _secrets import USER_AGENT_EMAIL, SEC_FILINGS_REPO_FOLDER, LOG_FOLDER
from secfilings import report_date_range
from datetime import date
from secedgar import DailyFilings
from secedgar.client import NetworkClient

class SkipErrorNetworkClient(NetworkClient):
    @staticmethod
    async def fetch(link, session):
        """Asynchronous get request.

        Args:
            link (str): URL to fetch.
            session (aiohttp.ClientSession): Asynchronous client session to use to perform
                get request.

        Returns:
            Content: Contents of response from get request.
        """
        try:
            async with await session.get(link) as response:
                contents = await response.read()
        except:
            logging.warn("Cannot download link: " + link)
            contents = ""

        return contents


def download_form4(report_date):
    def _filter_for_form4(filing_entry):
        return filing_entry.form_type.lower() == "4"

    # retry interval in seconds increases  2**n * backoff_factor
    daily_filings = DailyFilings(date=report_date,
                                 client=SkipErrorNetworkClient(
                                     user_agent=USER_AGENT_EMAIL,
                                     retry_count=4,
                                     backoff_factor=5.0,
                                     rate_limit=5),
                                 entry_filter=_filter_for_form4)
    daily_filings.save(directory=SEC_FILINGS_REPO_FOLDER)


def main(report_date: date):
    try:
        download_form4(report_date)
    except Exception as e:
        logging.exception(str(e))
    else:
        logging.info("Download Completed for report date: " + str(report_date))


if __name__ == '__main__':
    logging.basicConfig(filename=os.path.join(LOG_FOLDER, 'download_daily_form4.log'),
                        format='%(asctime)s - %(message)s', level=logging.INFO)

    # export SEC_REPORT_DATE_RANGE=2022-01-01:2022-01-10 && python download_daily_form4.py
    for report_date in report_date_range():
        if report_date.weekday() > 4:
            logging.info(f"Skipping weekend. Date: {report_date}")
            continue
        logging.info(f"Downloading file for date: {report_date}")
        try:
            download_form4(report_date)
        except Exception as e:
            logging.exception(str(e))
        else:
            logging.info(
                "Download Completed for report date: " + str(report_date))
    logging.info("All done!")
