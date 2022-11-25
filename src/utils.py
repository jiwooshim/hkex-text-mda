"""
Utilities script. Includes argparse, logging, global variables.
Last updated: July 2022
"""
import os
import pandas as pd
import PyPDF2
import re
import os
from collections import Counter
import string
import pikepdf
import random
import difflib
import time
import sqlite3
import argparse
import sys
from datetime import datetime, timedelta
import logging
import requests
import json
import random


def get_stockId(stock_code):
    logger.info(f"get stockId for stock_code: {stock_code}")
    count=0
    while True:
        count += 1
        try:
            res = requests.get(
                f"https://www1.hkexnews.hk/search/prefix.do?&callback=callback&lang=EN&type=A&name={str(int(stock_code))}&market=SEHK")
            res.raise_for_status()
            break
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTPError occurred: {e}, count: {count}")
            time.sleep(random.randint(5, 15))
            if count > 10:
                logger.error(f"HTTPError occurred 10+ times.")
                raise f"HTTPError occurred 10+ times."
    res = res.text.replace('callback(', '').replace(";", "")[:-2].strip()

    res_json = json.loads(res)
    stockId = res_json['stockInfo'][0]['stockId']
    code = res_json['stockInfo'][0]['code']
    name = res_json['stockInfo'][0]['name']
    logger.info(f"result – stockId: {stockId} code: {code} name: {name}")
    time.sleep(random.randint(5, 50)*0.01)

    return stockId


parser = argparse.ArgumentParser(description="This script extracts Table of Contents from PDF files using keywords."
                                             " Tested on Hong Kong Stock Exchange (HKEX) reports.")
parser.add_argument("--download_path", "-dp", required=False, help='Absolute directory where you want to save your '
                                                                   'file. Default: ./reports_data')
parser.add_argument("--stock_code", "-sc", required=False, help="Particular stock code of a company of interest, "
                                                                "separated by commas. ex. '700' for TENCENT, '9988' "
                                                                "for BABA. '700,9988' for both.")
parser.add_argument("--report_type", "-t2", required=True, help='-2 => All Financial Statements/ESG Information, '
                                                                '40100 => Annual Report, 40200 => Semi-annual Report '
                                                                '40300 => Quarterly Report 40400 => ESG '
                                                                'Information/Report')
parser.add_argument("--from_date", "-fd", required=True, help="From date of the report to be downloaded. "
                                                              "Format='yyyymmdd' for days, 'yyyymm' for months, and "
                                                              "'yyyy' for years. For example, '2018' will include all "
                                                              "reports from 2018 to the year specified in --to_date. "
                                                              "NOTICE: The format of --from_date and --to_date should "
                                                              "match."
                                                              "NOTICE2: The earliest date value limit is 2007 June.")
parser.add_argument("--to_date", "-td", required=True, help="To date of the report to be downloaded. "
                                                            "Format='yyyymmdd' for days, 'yyyymm' for months, and "
                                                            "'yyyy' for years. For example, '2018' will include all "
                                                            "reports up to 2018 from the year specified in --from_date."
                                                            " NOTICE: The format of --from_date and --to_date should "
                                                            "match")
args = parser.parse_args(sys.argv[1:])

module_start_time = time.time()
today = datetime.strftime(datetime.today(), "%Y%m%d")

module_path = os.path.dirname(os.path.abspath(__file__))
if args.download_path:
    download_path = args.download_path
else:
    download_path = os.path.join(module_path, 'reports_data')
if not os.path.exists(download_path):
    os.makedirs(download_path)
reports_path = os.path.join(download_path, 'hkex_reports')
mda_path = os.path.join(download_path, 'hkex_reports_mda')
mda_text_path = os.path.join(download_path, 'hkex_reports_mda_text')
metadata_db = sqlite3.connect(os.path.join(download_path, f'metadata_{today}.db'))


"""logger set up"""
log_path = os.path.join(download_path, 'hkex-text.log')

logger = logging.getLogger('hkex-text')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s - %(message)s', datefmt='%Y%m%d %H:%M:%S')

file_handler = logging.FileHandler(log_path)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)
file_handler.set_name('my_file_handler')
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.DEBUG)
console_handler.set_name('my_console_handler')
logger.addHandler(console_handler)

ts = time.time()
logger.info('=' * 65)
logger.info('Analysis started at {0}'.format(datetime.fromtimestamp(ts).strftime('%Y%m%d %H:%M:%S')))
logger.info('Command line:\t{0}'.format(sys.argv[0]))
logger.info('Arguments:\t\t{0}'.format(' '.join(sys.argv[:])))
logger.info('=' * 65)
"""logger set up"""


"""Below codes are required when downloading the reports. Uncomment as needed or pass in arguments from the terminal."""
if args.stock_code:
    stockCodeList = args.stock_code.split(',')
    stockIdList = [get_stockId(stockCode) for stockCode in stockCodeList]
else:
    stockCodeList = []
    stockIdList = [-1]

# t1codeVal = -2 ## => All
t1codeVal = 40000 ## => Financial Statements/ESG Information

# t2codeVal = -2 ## => All (under Financial Statements/ESG Information if t1codeVal == 40000 and entire data if t1codeVal == -2)
# t2codeVal = 40100 ## => Annual Report (under Financial Statements/ESG Information)
# t2codeVal = 40200 ## => Semi-annual Report (under Financial Statements/ESG Information)
# t2codeVal = 40300 ## => Quarterly Report (under Financial Statements/ESG Information)
# t2codeVal = 40400 ## => Environment, Social and Governance Information/Report (under Financial Statements/ESG Information)
t2codeVal = args.report_type

from_date = args.from_date
to_date = args.to_date
if len(from_date) == 8 and len(to_date) == 8:
    if (datetime.strptime(to_date, '%Y%m%d') - datetime.strptime(from_date, '%Y%m%d')) > timedelta(days=365):
        raise Exception("Date range from_date to to_date should be less than 365 days. Please use monthly or yearly "
                        "range if you need data from a range bigger than 1 year.")

if len(stockIdList) > 0:
    logger.info(f"Selected stockCode list: {stockCodeList}")
    logger.info(f"Selected stockId list: {stockIdList}")

if t2codeVal == '-2':
    logger.info("Selected t2code==-2, Processing 'All' report types under 'Financial Statements/ESG Information'.")
elif t2codeVal == '40100':
    logger.info("Selected t2code==40100, Processing 'Annual Report' report type")
elif t2codeVal == '40200':
    logger.info("Selected t2code==40200, Processing 'Semi-annual Report' report type")
elif t2codeVal == '40300':
    logger.info("Selected t2code==40300, Processing 'Quarterly Report' report type")
elif t2codeVal == '40400':
    logger.info("Selected t2code==40400, Processing 'ESG Information/Report' report type")
logger.info(f"Selected date range is from {from_date} to {to_date}")
logger.info('=' * 65)


def init_db():
    cur = metadata_db.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS metadata 
                (file_info text, file_name text, file_path text, month text, date_time text, stock_code integer, 
                stock_name text, title text, news_id integer, short_text text, long_text text, total_count integer, 
                file_type text, file_link text, dod_web_path text
                )""")
    cur.close()


def init_db_mda():
    cur = metadata_db.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS metadata_mda 
                (file_name text, file_path text, month text, ticker text, outline text, mda_extracted text, 
                mda_title_best_match text, status text
                )""")
    cur.close()


def init_db_mda_text():
    cur = metadata_db.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS metadata_mda_text 
                (file_name text, file_path text, month text, eng_extracted text, chi_extracted text
                )""")
    cur.close()


def get_filenames(input_folder):
    sublist = os.listdir(input_folder)
    sublist.sort()
    all_files_list = []
    current_files_list = []
    file_tree = {}
    for item in sublist:
        new_input_folder = os.path.join(input_folder, item)
        if os.path.isdir(new_input_folder):
            new_files_list, new_file_tree = get_filenames(new_input_folder)
            current_files_list.append(new_file_tree)
            all_files_list += new_files_list
        else:
            current_files_list.append(item)
            all_files_list += [item]
    file_tree.update({os.path.split(input_folder)[1]: current_files_list})
    return all_files_list, file_tree


def reset_eof(pdf_file):
    with open(pdf_file, 'rb') as p:
        txt = (p.readlines())
    actual_line = len(txt)
    for i, x in enumerate(txt[::-1]):
        if b'%%EOF' in x:
            actual_line = len(txt)-i-1
            break
    txtx = txt[:actual_line] + [b'%%EOF\r\n']
    pdf_file_newname = pdf_file+'fixedeof'
    with open(pdf_file_newname, 'wb') as f:
        f.writelines(txtx)
    return pdf_file_newname


def cleanse_outline(input_list):
    new_list = []
    for outline in input_list:
        new_outline = re.sub(r'([0-9]{1,2}. ).+', '', outline)
        new_outline = re.sub(r'.+(\(.+\))', '', new_outline)
        new_outline = new_outline.strip()
        new_list.append(new_outline)

    return new_list


def mda_exists(outlines_list):
    ## Find management's discussion and analysis from a list of outlines
    for outline in outlines_list:
        match = difflib.get_close_matches(outline, 'Management\'s Discussion and Analysis')
        if len(match) > 0:
            return True
    return False


def group_outlines(parentFolder='hkex_reports', interval='yearly'):
    folderList = os.listdir(parentFolder)
    folderList.sort()
    base_year = 0000
    yearly_outline_list = []
    error_count = 0
    df = pd.DataFrame()
    yearly_file_count = 0

    for idx, folder in enumerate(folderList):
        if int(folder.split("_")[0]) < 2007:
            continue
        print(folder)
        year = folder[:4]
        file_count = 0

        fileList = os.listdir(os.path.join(parentFolder, folder))
        pdf_outline_list = []
        for file in fileList:
            if not file.lower().endswith('pdf'):
                continue
            file_count += 1

            pdf_name = os.path.join(parentFolder, folder, file)
            try:
                pdf_outlines = list(PdfOutline(open(pdf_name, 'rb')).getDestinationPageNumbers().keys())
                # pdf_outlines = cleanse_outline(pdf_outlines)

                # if not mda_exists(pdf_outlines):
                PdfOutline(open(pdf_name, 'rb')).search_keyword('Management\'s Discussion and Analysis')


                pdf_outline_list += pdf_outlines
            except PyPDF2.utils.PdfReadError as e:
                if 'file has not been decrypted' in str(e):
                    try:
                        pdf = pikepdf.open(pdf_name)
                        os.rename(pdf_name, os.path.join(parentFolder, folder, file+'-OLD'))
                        pdf.save(pdf_name)

                        pdf_outlines = list(PdfOutline(open(pdf_name, 'rb')).getDestinationPageNumbers().keys())
                        pdf_outline_list += pdf_outlines
                    except Exception as e:
                        print(e)
                        error_count += 1
                else:
                    print(e)
                    error_count += 1
            except Exception as e:
                print(e)
                error_count += 1

        if interval == 'monthly':
            counted_list = count_outlines(pdf_outline_list, year)
            counted_list.insert(0, ('fileCount', file_count))
            counted_list.insert(0, ('month', folder))
            monthly_df = pd.DataFrame(dict(counted_list), index=[0])
            df = pd.concat([df, monthly_df], axis=0, ignore_index=True)

        if interval == 'yearly':
            if year == base_year:
                yearly_outline_list += pdf_outline_list
                yearly_file_count += file_count
                if idx+1 == len(folderList):
                    counted_list = count_outlines(yearly_outline_list, year)
                    counted_list.insert(0, ('fileCount', yearly_file_count))
                    counted_list.insert(0, ('year', base_year))
                    yearly_df = pd.DataFrame(dict(counted_list), index=[0])
                    df = pd.concat([df, yearly_df], axis=0, ignore_index=True)
            else:
                if len(yearly_outline_list) != 0:
                    counted_list = count_outlines(yearly_outline_list, year)
                    counted_list.insert(0, ('fileCount', yearly_file_count))
                    counted_list.insert(0, ('year', base_year))
                    yearly_df = pd.DataFrame(dict(counted_list), index=[0])
                    df = pd.concat([df, yearly_df], axis=0, ignore_index=True)
                yearly_outline_list = pdf_outline_list
                yearly_file_count = file_count
                base_year = year
    return df


def count_outlines(outline_list, year):
    cleaned_list = []
    for element in outline_list:
        element = element.decode('iso-8859-1').strip()
        element = string.capwords(element)
        element = re.sub('(\\r)', '', element)
        element = re.sub('.?\x80\x99', "'", element)
        element = re.sub('.?\x80\x93', "-", element)
        element = re.sub('’', '\'', element)
        element = re.sub(r'([0-9]{1,2}\.)', '', element)  ## ex) 2. Auditor's Report
        element = re.sub(r'(\(.+\))', '', element)  ## ex) Auditor's Report (sample_text)
        element = re.sub(r'([0-9]\.[0-9])', '', element)  ## 3.1 Auditor's Report
        element = re.sub(r'([0-9])', '', element)  ## 3 Auditor's Report
        element = re.sub(r'([A-Z]\.)', '', element)  ## A: Auditor's Report
        element = re.sub(r'(Section\ [0-9])', '', element)  ## Section 2 Auditor's Report
        element = re.sub(r'(Chapter\ [0-9])', '', element)  ## Chapter 2 Auditor's Report
        roman_numbers_pattern = r"\b(?=[MDCLXVIΙ])M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})([IΙ]X|[IΙ]V|V?[IΙ]{0,3})\b\.?"
        element = re.sub(roman_numbers_pattern, '', element)  ## IV Auditor's Report

        element = element.strip()
        cleaned_list.append(element)


    # counted_list = sorted(Counter(cleaned_list).items())
    counted_list = Counter(cleaned_list).most_common()

    return counted_list


def count_outlined_pdf(parentFolder='hkex_reports'):
    folderList = os.listdir(parentFolder)
    folderList.sort()

    df = pd.DataFrame(columns=["year", "totalCount", "successCount", "noneCount", "errorCount", "noneList", "errorList"])
    for folder in folderList:
        if int(folder.split("_")[0]) <= 2007:
            continue
        print(folder)
        totalCount = 0
        successCount = 0
        noneCount = 0
        errorCount = 0

        fileList = os.listdir(os.path.join(parentFolder, folder))
        noneList = []
        errorList = []
        errorDetail = []
        for file in fileList:
            if not file.lower().endswith('pdf'):
                continue
            totalCount += 1
            pdfname = os.path.join(parentFolder, folder, file)
            try:
                pdf = PdfOutline(open(pdfname, 'rb'))

                if len(pdf.getDestinationPageNumbers().items()) == 0:
                    noneCount += 1
                    noneList.append(file)
                else:
                    successCount += 1
            except PyPDF2.utils.PdfReadError as e:
                if 'eof' in e.args[0].lower():
                    pdf = PdfOutline(open(reset_eof(pdfname), 'rb'))
                    if len(pdf.getDestinationPageNumbers().items()) == 0:
                        noneCount += 1
                        noneList.append(file)
                    else:
                        successCount += 1
                else:
                    errorCount += 1
                    errorList.append(file)
                    errorDetail.append(e)
            except Exception as e:
                errorCount += 1
                errorList.append(file)
                errorDetail.append(e)


        year = folder[:5] + folder.split("_")[1].zfill(2)
        new_row = {"year": year,
                   "totalCount": totalCount,
                   "successCount": successCount,
                   "noneCount": noneCount,
                   "errorCount": errorCount,
                   "noneList": noneList,
                   "errorList": errorList,
                   "errorDetail": errorDetail}
        df = df.append(new_row, ignore_index=True)

    # template = '%-5s  %s'
    # print(template % ('page', 'title'))
    # for p, t in sorted([(v, k) for k, v in pdf.getDestinationPageNumbers().items()]):
    #     print(template % (p+1, t))

    print('')
    df.to_csv('/home/jiwooshim/PycharmProjects/Others/old/hkex_data/pdf_outline_re.csv')
    df.to_pickle('/home/jiwooshim/PycharmProjects/Others/old/hkex_data/pdf_outline_re.pickle')



def count_doc_types(fromDate, toDate, parentFolder='hkex_reports', interval='monthly'):
    folderList = os.listdir(parentFolder)
    folderList.sort()
    base_year = 0000
    yearly_type_list = []
    yearly_file_count = 0
    errorCount = 0
    other_types_list = []
    df = pd.DataFrame()
    doctypes = ['annual', 'statement', 'interim', 'esg', 'announcement', 'meetings', 'form',
               'movement', 'circular', 'notice', 'change', 'result']

    for idx, folder in enumerate(folderList):
        if int(folder.split("_")[0]) > toDate + 1:
            continue
        print(folder)
        year = folder[:4]
        fileList = os.listdir(os.path.join(parentFolder, folder))
        file_type_list = []
        fileCount = 0

        for file in fileList:
            # if not file.lower().endswith('pdf'):
            #     continue
            fileCount += 1
            file_title = "_".join(file.split("_")[1:]).split(".pdf")[0].split('.PDF')[0].replace('.htm', '')\
                .replace('.HTM', '').replace('-', ' ').strip()
            file_type = 'other'
            for t in doctypes:
                if t not in file_title.lower():
                    continue
                file_type = t
                break

            file_type_list.append(file_type)
            if file_type == 'other':
                other_types_list.append(file_title)

        if interval == 'monthly':
            counted_list = Counter(file_type_list).most_common()
            counted_list.insert(0, ('fileCount', fileCount))
            counted_list.insert(0, ('month', folder))
            counted_list.append(('others_title', f"[{', '.join(random.sample(other_types_list, 20))}]"))
            monthly_df = pd.DataFrame(dict(counted_list), index=[0])
            df = pd.concat([df, monthly_df], axis=0, ignore_index=True)

        elif interval == 'yearly':
            if year == base_year:
                yearly_type_list += file_type_list
                yearly_file_count += fileCount
            else:
                if len(yearly_type_list) != 0:
                    counted_list = Counter(yearly_type_list).most_common()
                    counted_list.insert(0, ('fileCount', yearly_file_count))
                    counted_list.insert(0, ('year', base_year))
                    counted_list.append(('others_title', f"[{', '.join(random.sample(other_types_list, 20))}]"))
                    yearly_df = pd.DataFrame(dict(counted_list), index=[0])
                    df = pd.concat([df, yearly_df], axis=0, ignore_index=True)
                yearly_file_count = fileCount
                yearly_type_list = file_type_list
                base_year = year
    return df

