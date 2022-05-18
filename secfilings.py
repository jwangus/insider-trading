import os
import logging

from secedgar.parser import MetaParser
from _secrets import SEC_FILINGS_REPO_FOLDER, SP500_COMPANY_CSV
import xml.etree.ElementTree as ET
from datetime import date, timedelta
import pandas as pd
import numpy as np


def get_report_date_range_from_env():
    env_name = "SEC_REPORT_DATE_RANGE"
    if env_name in os.environ:
        return os.environ[env_name]
    else:
        return ""


def report_date_range():
    return report_date_range_from_str(get_report_date_range_from_env())


def report_date_range_from_str(date_range: str = None):
    if date_range:
        dates = [date.fromisoformat(d) for d in date_range.split(":")]
        min_date = min(dates)
        max_date = max(dates)
    else:
        min_date = max_date = previous_weekday()

    num_days = int((max_date - min_date) / timedelta(days=1))

    return (min_date + timedelta(days=n) for n in range(0, num_days+1))


def previous_weekday():
    # today weekday    = 0, 1, 2, 3, 4, 5, 6
    # previous weekday = 4, 0, 1, 2, 3, 4, 4
    today_weekday = date.today().weekday()
    if today_weekday == 0:
        return date.today() - timedelta(days=3)
    elif today_weekday == 6:
        return date.today() - timedelta(days=2)
    else:
        return date.today() - timedelta(days=1)


def filings_path(report_date):
    return os.path.join(SEC_FILINGS_REPO_FOLDER, f"{report_date:%Y%m%d}")


def process_download(report_date):
    """ process all downloaded files for the day
    """
    all_filings = find_all_filing_files(report_date)

    p = MetaParser()
    for f in all_filings:
        p.process(f)

    return len(all_filings)


def find_all_filing_files(report_date, file_extension='txt'):
    all_filings = []
    for root, _, filenames in os.walk(filings_path(report_date)):
        for filename in filenames:
            if filename.endswith(f".{file_extension}"):
                all_filings.append(os.path.join(root, filename))
    logging.info(f"number of fillings: {len(all_filings)}")
    return all_filings


def map_relationship(r):
    relation_elements = r.findall(
        "./reportingOwner/reportingOwnerRelationship/")
    relationship = ""
    title = ""
    for e in relation_elements:
        if e.tag.startswith("is") and e.text.lower().strip() in ("1", "yes", "true", "y"):
            relationship = e.tag[2:]
        elif e.tag == "officerTitle":
            title = e.text
    return relationship, title


def parse_form4_xml(s, filename):
    logging.debug(f"reading file: {filename}")
    with open(filename, "r") as f:
        # filter out invalid XML tags
        root = ET.fromstringlist(
            filter(lambda l: not l.startswith(("<XML>", "</XML>")), f.readlines()))
    logging.debug(str(root))

    cik = root.find("./issuer/issuerCik").text
    ticker = root.find("./issuer/issuerTradingSymbol").text
    name = root.find("./reportingOwner/reportingOwnerId/rptOwnerName").text
    relationship, title = map_relationship(root)
    path_root = "./nonDerivativeTable/"
    for ele in root.findall(path_root):
        e_tx_date = ele.find("./transactionDate/value")
        e_tx_code = ele.find("./transactionCoding/transactionCode")
        if e_tx_date is None or e_tx_code is None:
            continue

        s["cik"].append(cik)
        s["ticker"].append(ticker)
        s["name"].append(name)
        s["relationship"].append(relationship)
        s["title"].append(title)

        s["tx_date"].append(date.fromisoformat(e_tx_date.text[:10]))
        s["tx_code"].append(e_tx_code.text)
        e = ele.find("./transactionAmounts/transactionShares/value")
        s["tx_share"].append(None if e is None else float(e.text))
        e = ele.find("./transactionAmounts/transactionPricePerShare/value")
        s["tx_price"].append(None if e is None else float(e.text))
        e = ele.find(
            "./postTransactionAmounts/sharesOwnedFollowingTransaction/value")
        s["share_post_tx"].append(None if e is None else float(e.text))


def generate_daily_summary_report_data(report_date):
    def filter_form4(n: str):
        n = n.lower()
        return "form4" in n or "doc4" in n

    all_form4_xml = filter(
        filter_form4, find_all_filing_files(report_date, "xml"))

    s = {"cik": [], "ticker": [], "name": [], "relationship": [], "title": [],
         "tx_date": [], "tx_code": [], "tx_share": [], "tx_price": [], "share_post_tx": []}

    # sometimes, identicle form 4 would appear under owner's CIK and company's CIK.
    # So we want to filter out the duplicate.
    folder_seen = []
    for filename in all_form4_xml:
        folder_name = os.path.basename(os.path.dirname(filename))
        if folder_name not in folder_seen:
            folder_seen.append(folder_name)
            try:
                parse_form4_xml(s, filename)
            except:
                logging.warn("Cannot parse file: " + filename)

    return s


def capitalize_word(n):
    donot_cap_words = ["VP", "EVP", "CEO", "II",
                       "III", "CFO", "SVP", "C.E.O", "CCO"]
    return ' '.join(i if i in donot_cap_words else i.capitalize() for i in n.split())


def format_cik(cik):
    return f'<a href="https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK={cik}">{cik}</a>'


def format_ticker_html(t):
    return f'<a href="https://finance.yahoo.com/quote/{t}">{t}</a>'


def format_buysell(c):
    code = {"P": "Buy", "S": "Sell"}
    return code[c] if c in code else "Unknown"


def calc_name_title(row):
    cell_text = "/".join([capitalize_word(row[("name", "")]),
                         row[("relationship", "")], capitalize_word(row[("title", "")])])
    cell_text = cell_text.replace("Chief Financial Officer", "CFO").replace(
        "Chief Executive Officer", "CEO")
    cik = row[("cik", "")]
    return f'<a href="https://www.sec.gov/cgi-bin/own-disp?action=getissuer&CIK={cik}">{cell_text}</a>'


def calc_trade_date_range(row):
    d_min = str(row[('tx_date', 'min')])
    d_max = str(row[('tx_date', 'max')])

    return d_min[5:] if d_min == d_max else "/".join([d_min[5:], d_max[5:]])


def calc_change_in_position(row):
    share_base = row[("pre_share", "min")] if row[(
        "tx_code", "")] == "P" else row[("pre_share", "max")]

    if share_base == 0:
        return "New"
    else:
        return '{:,.1f}%'.format(row[("tx_share", "sum")]/share_base*100)


def calc_pre_share(row):
    if row.tx_code not in ("P", "S"):
        raise ValueError("Cannot handle tx_code: " + row.tx_code)

    return row.share_post_tx - row.tx_share if row.tx_code == "P" else row.share_post_tx + row.tx_share


def summary_by_ticker(df):
    df_summary = group_by_ticker(df)

    df_output = pd.DataFrame()

    df_output["cik"] = df_summary.cik.map(format_cik)
    df_output["Ticker"] = df_summary.ticker.map(format_ticker_html)
    df_output["Buy/Sell"] = df_summary["tx_code"].map(format_buysell)
    df_output["Trade Dollar"] = df_summary[(
        "amt", "sum")].map('{:,.0f}'.format)
    df_output["Trade Share"] = df_summary[(
        "tx_share", "sum")].map('{:,.0f}'.format)
    df_output["Average Price"] = (df_summary[(
        "amt", "sum")]/df_summary[("tx_share", "sum")]).map('{:,.2f}'.format)

    return df_output


def group_by_ticker(df):
    df_summary = df.groupby(["cik", "ticker", "tx_code", "buy_sell_order"]).agg(
        {"amt": ["sum"], "tx_share": ["sum"]})
    df_summary.sort_values(["buy_sell_order", ("amt", "sum")],
                           ascending=False, inplace=True)
    df_summary.reset_index(inplace=True)
    return df_summary


def summary_by_insider(df):
    df_summary = group_by_insider(df)

    df_output = pd.DataFrame()

    df_output["Name/Title"] = df_summary.apply(calc_name_title, axis=1)

    df_output["Ticker"] = df_summary["ticker"].map(format_ticker_html)
#    df_output["Added to SP500 on"] = df_summary["date_added_to_sp500"]
    df_output["SP500 Sector"] = df_summary[("sector", "max")].map(
        lambda n: "Not in SP500" if n is np.nan else n)

    df_output["Buy/Sell"] = df_summary["tx_code"].map(format_buysell)

    df_output["Trade Amount"] = df_summary[(
        "amt", "sum")].map('{:,.0f}'.format)
    df_output["% Change In Position"] = df_summary.apply(
        calc_change_in_position, axis=1)

    df_output["Average Price"] = (df_summary[(
        "amt", "sum")]/df_summary[("tx_share", "sum")]).map('{:,.2f}'.format)
    df_output["Trade Date/Range"] = df_summary.apply(
        calc_trade_date_range, axis=1)

    return df_output, df_summary


def group_by_insider(df):
    df_summary = df.groupby(["cik", "name", "relationship", "title", "ticker", "tx_code", "buy_sell_order"]).agg(
        {"amt": ["sum"], "tx_share": ["sum"], "pre_share": ["min", "max"], "tx_date": ["min", "max"], "tx_price": ["min", "max"], "sector": ["max"]})
    df_summary.sort_values(["buy_sell_order", ("amt", "sum")],
                           ascending=False, inplace=True)
    df_summary.reset_index(inplace=True)
    return df_summary


def generate_daily_summary_report(report_data):
    df = create_raw_df(report_data)

    df_ticker = summary_by_ticker(df.copy())
    df_insider, df_insider_raw = summary_by_insider(df.copy())

    return df_ticker, df_insider, df, df_insider_raw


def create_raw_df(report_data):
    df = pd.DataFrame(report_data)

    df = df[(df.tx_code == "P") | (df.tx_code == "S")]
    df["amt"] = df.tx_share*df.tx_price
    df["pre_share"] = df.apply(calc_pre_share, axis=1)
    df["buy_sell_order"] = df.tx_code.map(lambda c: 1 if c == "P" else 0)
    df["cik"] = df["cik"].map(lambda x: int(x))

    # add SP500 related columns
    df_sp500 = pd.read_csv(SP500_COMPANY_CSV)
    df_sp500 = df_sp500[['CIK', 'GICS Sector', 'Date first added']]
    df_sp500.columns = ['cik', 'sector', 'date_added_to_sp500']

    return df.merge(df_sp500, on='cik', how='left')
