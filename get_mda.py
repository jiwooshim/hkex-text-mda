"""
This script finds and extracts only MD&A from reports using their outline, then saves into a separate PDF.
Last updated: July 2022
"""
import os.path
import pandas as pd
import PyPDF2
from pathlib import Path
import os
import sqlite3
from thefuzz import fuzz, process
import fitz
import time
import traceback
import sys
from . import utils
from .utils import module_path, download_path, reports_path, mda_path, metadata_db
from .utils import module_start_time, t1codeVal, t2codeVal, from_date, to_date
from .utils import logger


def get_mda(source_path, saveDoc_mda):
    cur = metadata_db.cursor()

    folderList = os.listdir(source_path)
    folderList.sort()

    for folder in folderList:
        if os.path.isfile(os.path.join(source_path, folder)):
            continue
        if int(folder.split("_")[0]) < 2007:
            continue
        if "_mda" in folder or "_mda_test" in folder:
            continue
        if folder+"_mda" in folderList:
            continue
        logger.info(f"Processing folder: {folder}")

        fileList = os.listdir(os.path.join(source_path, folder))

        for file in fileList:
            if not file.lower().endswith('pdf'):
                continue
            pdfname = os.path.join(source_path, folder, file)
            outline = 'error'
            mda_extracted = 'false'
            mda_title = None
            dest_fpath = ''
            try:
                doc = fitz.open(pdfname)
                doc_outline = doc.get_toc()
                doc_outline = [element for element in doc_outline if element[0] == 1]

                if len(doc_outline) == 0:
                    outline = 'false'
                    status = 'none'
                else:
                    outline = 'true'
                    mda_match, start_page, end_page = find_mda_range(doc_outline)
                    if isinstance(mda_match, list):
                        status = "fail"
                        mda_title = ''
                        logger.info(f"{folder} : {file} : None : [Fail]")
                    elif start_page == -1 or end_page == -1:
                        status = "no_pageNum"
                        mda_title = mda_match
                        logger.info(f"{folder} : {file} : None : [pageNum does not exist]")
                    else:
                        mda_title = mda_match
                        source_fpath = os.path.join(source_path, folder, file)
                        dest_path = os.path.join(saveDoc_mda, folder)
                        dest_fpath = pdf_extract_range(source_fpath, mda_title, start_page, end_page, dest_path)
                        mda_extracted = "true"
                        status = "success"
                        logger.info(f"{folder} : {file} : {mda_match} : [Success]")

            except Exception as e:
                logger.debug(traceback.print_exception(*sys.exc_info()))
                status = "error"

            year = folder[:5] + folder.split("_")[1].zfill(2)
            ticker = file.split("_")[0]
            query = f'''INSERT INTO metadata_mda VALUES (
            "{file}", 
            "{dest_fpath}",                     
            "{folder}", 
            "{ticker}", 
            "{outline}", 
            "{mda_extracted}", 
            "{mda_title}", 
            "{status}")'''
            try:
                cur.execute(query)
            except sqlite3.OperationalError as e:
                logger.debug(traceback.print_exception(*sys.exc_info()))
    cur.close()


def find_mda_range(pdf_outline, simple_match=False):
    """
    Finds the range of MD&A section in a PDF file outline.
    :param pdf_outline: PDF outline in the format of (lvl(positive int), title(str), page(int)) if simple_match==False,
    or a flat list of outline titles if simple_match==True.
    :param simple_match: If True, it simply returns best match of MD&A title from pdf_outline.
    :return: best match keyword, start page num, end page num
    """
    keywords = ["Management Discussion and Analysis",
                "管理層討論及分析",
                "管理層討論與分析",
                "管理層論述與分析",
                "讨论分析",
                "管理層之討論及分析"
                ]

    all_matches = []
    only_successful_matches = []

    if simple_match:
        for keyword in keywords:
            mda_match, match_ratio = find_mda_match(keyword, pdf_outline)
            if not isinstance(mda_match, list):
                only_successful_matches.append([mda_match, match_ratio, None, None])
            all_matches.append([mda_match, match_ratio, None, None])
    else:
        for keyword in keywords:
            outline_titles_list = [element[1] for element in pdf_outline]
            mda_match, match_ratio = find_mda_match(keyword, outline_titles_list)
            if not isinstance(mda_match, list):
                start_page, end_page = find_section_range(mda_match, pdf_outline)
                only_successful_matches.append([mda_match, match_ratio, start_page, end_page])
            else:
                start_page = None
                end_page = None
            all_matches.append([mda_match, match_ratio, start_page, end_page])

    if len(only_successful_matches) == 0:
        return all_matches, None, None

    df_match_candidates = pd.DataFrame(only_successful_matches,
                                       columns=['keyword', 'fuzzRatio', 'startPage', 'endPage'])
    """Find the keyword with the highest fuzzRatio"""
    df_best_match = df_match_candidates[df_match_candidates['fuzzRatio'] == df_match_candidates['fuzzRatio'].max()]

    if simple_match:
        return df_best_match['keyword'][0], None, None

    return df_best_match['keyword'][0], df_best_match['startPage'][0], df_best_match['endPage'][0]


def find_mda_match(keyword, titles_list):
    best_match_list = [element[0] for element in process.extractBests(keyword, titles_list, limit=15, scorer=fuzz.partial_ratio)]

    for match in best_match_list:
        if isinstance(match, (bytes, bytearray)):
            b_match = match
            match = match.decode('utf-8')
        else:
            b_match = bytes(match, encoding='utf-8')

        """return None if the similarity ratio is below 60 """
        ratio = fuzz.partial_ratio(match.lower(), keyword.lower())
        if ratio < 60:
            continue

        ## return None if the matched keyword is in below keywords
        skip_keywords = ["consolidated statement", "consolidated income statement", "risk management", "senior management",
                         "management profile", "management team", "chairman and ceo", "financial statement",
                         "consolidated cash flow statement", "mission statement", "statement of financial position",
                         "our mission", "management and administration"]

        def skip_match(skip_keywords_list):
            for k in skip_keywords_list:
                if fuzz.partial_ratio(k.lower(), match.lower()) > 80:
                    return True
            return False

        if skip_match(skip_keywords):
            continue

        return match, ratio

    return best_match_list, None


def find_section_range(match, pdf_outline):
    pdf_outline_keys = [element[1] for element in pdf_outline]
    pdf_outline_values = [element[2] for element in pdf_outline]

    page_start = [t for t in pdf_outline if match in t][0][2]
    try:
        page_end = pdf_outline_values[pdf_outline_keys.index(match) + 1] - 1
    except IndexError:
        page_end = pdf_outline_values[-1]
    if page_end < page_start:
        page_end = page_start

    return page_start, page_end


def pdf_extract_range(pdf_fname, outline_title, page_start, page_end, saveDoc_mda):
    if not os.path.exists(saveDoc_mda):
        os.mkdir(saveDoc_mda)
    with open(pdf_fname, 'rb') as read_stream:
        pdf_reader = PyPDF2.PdfReader(read_stream)
        pdf_writer = PyPDF2.PdfWriter()
        for page_num in range(page_start - 1, page_end):
            pdf_writer.addPage(pdf_reader.getPage(page_num))

        dest_fpath = os.path.join(saveDoc_mda, f'{Path(pdf_fname).stem}_pages_{page_start}-{page_end}_{outline_title.replace(" ", "_")}.pdf')
        if os.path.exists(dest_fpath):
            logger.info(f"File already exists. Skipping: {dest_fpath}")
            return dest_fpath
        with open(dest_fpath, 'wb') as dest_file:
            pdf_writer.write(dest_file)
        return dest_fpath


def main():
    logger.info('Start MD&A extraction')
    script_start_time = time.time()
    utils.init_db_mda()
    if not os.path.exists(mda_path):
        os.makedirs(mda_path)
    get_mda(reports_path, saveDoc_mda=mda_path)
    metadata_db.commit()
    elapsed_time = time.time() - script_start_time
    logger.info(f"MD&A PDF extraction complete. Elapsed time: {elapsed_time}")
    logger.info('=' * 65)
    return True


if __name__ == "__main__":
    main()