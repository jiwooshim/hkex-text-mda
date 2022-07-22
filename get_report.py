"""
This script downloads reports according to the input parameters provided.
Last updated: July 2022
"""
import requests
import json
import math
import re
import os
import time
from datetime import datetime
import calendar
import sqlite3
from bs4 import BeautifulSoup
import traceback
import sys
from .utils import module_path, download_path, reports_path, mda_path, metadata_db
from .utils import module_start_time, t1codeVal, t2codeVal, from_date, to_date
from .utils import logger
from . import utils, get_mda


def get_pdf(html_doc, url, filepath):
    soup = BeautifulSoup(html_doc, 'html.parser')
    if 'The page requested may have been relocated, renamed or removed' in soup.text:
        return
    base_url = "/".join(url.split('/')[:-1])
    links_list = soup.find_all('a')
    if len(links_list) == 0:
        return

    mda_match = get_mda.find_mda_range([link.text for link in links_list], simple_match=True)[0]

    for link in links_list:
        if link.get('href') is None:
            continue
        section_url = f"{base_url}/{link.get('href')}"
        download = requests.get(section_url)
        section_name = link.text
        if not section_name == mda_match:
            continue
        if "/" in section_name:
            section_name = re.sub('/', '', section_name)

        with open(f"{filepath.replace('.txt', '')}_pages_n-n_{section_name.replace(' ', '')}.pdf", 'wb') as f:
            f.write(download.content)
            return


def download_report(fromDateVal, toDateVal, saveDoc, saveDoc_mda):
    cur = metadata_db.cursor()
    params = {'sortDir': 0, 'sortByOptions': 'DateTime', 'category': 0, 'market': 'SEHK', 'stockId': -1, 'documentType': -1,
              't1code': t1codeVal, 't2Gcode': -2, 't2code': t2codeVal, 'searchType': 0, 'title': '', 'lang': 'E',
              'rowRange': 100,
              'fromDate': fromDateVal, 'toDate': toDateVal,}

    r_getCnt = requests.get('https://www1.hkexnews.hk/search/titleSearchServlet.do', params=params)
    try:
        totalCnt = json.loads(r_getCnt.text)['recordCnt']
    except json.decoder.JSONDecodeError as e:
        logger.debug(traceback.print_exception(*sys.exc_info()))
        return
    logger.info(f"Total Record return: {totalCnt}")
    params['rowRange'] = int(math.ceil(totalCnt / 100.0)) * 100
    r_getRow = requests.get('https://www1.hkexnews.hk/search/titleSearchServlet.do', params=params)
    try:
        data = json.loads(r_getRow.text)
    except json.decoder.JSONDecodeError as e:
        logger.debug(traceback.print_exception(*sys.exc_info()))
        return
    logger.info(f"Still have next row: {data['hasNextRow']}")
    logger.info(f"Loaded Record: {data['loadedRecord']}")
    logger.info(f"Total Record: {data['recordCnt']}")
    rptList = json.loads(data['result'])
    for idx, row in enumerate(rptList):
        filetype = 'txt'
        if len(row['FILE_TYPE']):
            filetype = row['FILE_TYPE'].lower()
        stock_code_clean = re.sub(r'<.*?>', '.', row['STOCK_CODE'][:20])
        report_date = datetime.strftime(datetime.strptime(row['DATE_TIME'], '%d/%m/%Y %H:%M'), '%Y%m%d')
        title_clean = (row['TITLE'][:100] + '...' if len(row['TITLE']) > 100 else row['TITLE']).replace(' ', '-')
        filename = f"{stock_code_clean}_{row['NEWS_ID']}_{report_date}_{title_clean}.{filetype}"
        filename = re.sub(r'[\\/*?:"<>|]', "", filename)
        filepath = os.path.join(saveDoc, filename)
        if os.path.exists(filepath):
            logger.info(f"File already exists. Skipping: {filepath}")
            continue
        mdapath = os.path.join(saveDoc_mda, filename)

        base_url = 'https://www1.hkexnews.hk' + row['FILE_LINK']
        download = requests.get(base_url)
        if (filetype.lower() in ['htm', 'txt']) and ('annual' in filename.lower()):
            get_pdf(download.content, base_url, mdapath)

        with open(filepath, 'wb') as f:
            f.write(download.content)
            logger.info(f"{idx} : "
                        f"{re.sub(r'<.*?>', ',', row['STOCK_CODE'])} : "
                        f"{re.sub(r'<.*?>', ',', row['STOCK_NAME'])} : "
                        f"{row['TITLE']} : "
                        f"{filename} : "
                        f"[Downloaded]")

        query = f'''INSERT INTO metadata VALUES (
                "{row['FILE_INFO']}",
                "{filename}",
                "{filepath}", 
                "{os.path.split(saveDoc)[1]}",
                "{row['DATE_TIME']}", 
                "{row['STOCK_CODE']}", 
                "{row['STOCK_NAME']}", 
                "{re.sub('"', "'", row['TITLE'])}", 
                "{row['NEWS_ID']}", 
                "{row['SHORT_TEXT']}", 
                "{row['LONG_TEXT']}", 
                "{row['TOTAL_COUNT']}", 
                "{row['FILE_TYPE']}", 
                "{row['FILE_LINK']}", 
                "{row['DOD_WEB_PATH']}")'''
        try:
            cur.execute(query)
        except sqlite3.OperationalError as e:
            traceback.print_exception(*sys.exc_info())
    cur.close()


def main():
    logger.info('Start report download')
    script_start_time = time.time()
    utils.init_db()
    if len(from_date) == 8 and len(to_date) == 8:
        if not os.path.exists(reports_path):
            os.makedirs(reports_path)
        if not os.path.exists(mda_path):
            os.makedirs(mda_path)
        logger.info(f"Downloading from {from_date} to {to_date}")

        download_report(from_date, to_date, saveDoc=reports_path, saveDoc_mda=mda_path)
    elif len(from_date) == 6 and len(to_date) == 6:
        from_year = int(from_date[:4])
        to_year = int(to_date[:4])
        from_month = int(from_date[4:6])
        to_month = int(to_date[4:6])
        for year in range(from_year, to_year + 1):
            for month in range(1, 12 + 1):
                if year == from_year:
                    if month < from_month:
                        continue
                if year == to_year:
                    if month > to_month:
                        continue
                month_end_date = calendar.monthrange(year, month)[1]
                month = str(month).zfill(2)
                logger.info(f"Downloading y {year}, m {month}")
                saveDoc = os.path.join(reports_path, f'{year}_{month}')
                saveDoc_mda = os.path.join(mda_path, f'{year}_{month}')
                if not os.path.exists(saveDoc):
                    os.makedirs(saveDoc)
                if not os.path.exists(saveDoc_mda):
                    os.makedirs(saveDoc_mda)
                fromDateVal = f'{year}{month}01'
                toDateVal = f'{year}{month}{month_end_date}'
                download_report(fromDateVal, toDateVal, saveDoc=saveDoc, saveDoc_mda=saveDoc_mda)
    elif len(from_date) == 4 and len(to_date) == 4:
        from_year = int(from_date[:4])
        to_year = int(to_date[:4])
        for year in range(from_year, to_year+1):
            for month in range(0, 12+1):
                month_end_date = calendar.monthrange(year, month)[1]
                month = str(month).zfill(2)
                logger.info(f"Downloading y {year}, m {month}")
                saveDoc = os.path.join(reports_path, f'{year}_{month}')
                saveDoc_mda = os.path.join(mda_path, f'{year}_{month}')
                if not os.path.exists(saveDoc):
                    os.makedirs(saveDoc)
                if not os.path.exists(saveDoc_mda):
                    os.makedirs(saveDoc_mda)
                fromDateVal = f'{year}{month}01'
                toDateVal = f'{year}{month}{month_end_date}'
                download_report(fromDateVal, toDateVal, saveDoc=saveDoc, saveDoc_mda=saveDoc_mda)
    else:
        raise Exception("--from_date and --to_date not in length of 8, 6, or 4. Please revise your entry. For help, "
                        "refer to the argument help description.")
    metadata_db.commit()
    elapsed_time = time.time() - script_start_time
    logger.info(f"Download complete. Elapsed time: {elapsed_time}")
    logger.info('=' * 65)
    return True


if __name__ == "__main__":
    main()

