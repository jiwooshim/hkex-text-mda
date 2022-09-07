"""
This script extracts and exports text from MD&A sections identified from extract_mda_automation_test.py.
last updated: July 2022
"""
import os.path
from bs4 import BeautifulSoup
import re
import os
from collections import Counter
import fitz
import sqlite3
import sys
import traceback
import time
from . import utils
from .utils import module_path, download_path, reports_path, mda_path, mda_text_path, metadata_db, subfolders_required
from .utils import module_start_time, t1codeVal, t2codeVal, from_date, to_date
from .utils import logger


def get_mda_text(source_path, saveDoc_text):
    cur = metadata_db.cursor()

    folderList = os.listdir(source_path)
    folderList.sort()

    for idx, folder in enumerate(folderList):
        if not subfolders_required:
            fileList = folderList
            folder = ''
        else:
            if os.path.isfile(os.path.join(source_path, folder)):
                continue
            if int(folder.split("_")[0]) < 2007:
                continue
            logger.info(f"Processing folder: {folder}")
            fileList = os.listdir(os.path.join(source_path, folder))

        for file in fileList:
            if not file.lower().endswith('pdf'):
                continue
            source_fpath = os.path.join(source_path, folder, file)
            dest_path = os.path.join(saveDoc_text, folder)
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)
            eng_text_list, chi_text_list = extract_text(source_fpath)

            """English text extraction"""
            eng_fname = file.replace(".pdf", "_ENG.txt")
            eng_fpath = os.path.join(dest_path, eng_fname)
            if os.path.exists(eng_fpath):
                continue
            eng_text = clean_text(eng_text_list, language='eng')
            if len(eng_text) > 0:
                with open(eng_fpath, 'w') as f:
                    f.write(eng_text)
                    eng_extracted = True
                    logger.info(f"{folder} : {eng_fname} : ENG : [Success]")
            else:
                logger.info(f"{folder} : {eng_fname} : ENG : [Fail]")
                eng_extracted = False

            """Chinese text extraction"""
            chi_fname = file.replace(".pdf", "_CHI.txt")
            chi_fpath = os.path.join(dest_path, chi_fname)
            if os.path.exists(chi_fpath):
                continue
            chi_text = clean_text(chi_text_list, language='chi')
            if len(chi_text) > 0:
                with open(chi_fpath, 'w') as f:
                    f.write(chi_text)
                    logger.info(f"{folder} : {chi_fname} : CHI : [Success]")
                    chi_extracted = True
            else:
                logger.info(f"{folder} : {chi_fname} : CHI : [Failed]")
                chi_extracted = False

            query = f'''INSERT INTO metadata_mda_text VALUES (
                    "{file.replace('.pdf', '')}", 
                    "{saveDoc_text}", 
                    "{folder}", 
                    "{eng_extracted}", 
                    "{chi_extracted}")'''
            try:
                cur.execute(query)
            except sqlite3.OperationalError as e:
                logger.debug(traceback.print_exception(*sys.exc_info()))
        if not subfolders_required:
            break
    cur.close()


def clean_text(text_list, language):
    if language == 'chi':
       duplicates_removed = remove_duplicates(text_list)
       cleaned_text = "".join(duplicates_removed)
    elif language == 'eng':
        duplicates_removed = remove_duplicates(text_list)
        standalones_removed = remove_standalone_text(duplicates_removed)
        capitals_removed = remove_capitalized_text(standalones_removed)
        cleaned_text = "".join(capitals_removed)
        if re.findall(r'[\u4e00-\u9fa5！？。，《》「」『』－（）【】]+', cleaned_text) is not None:
            cleaned_text = re.sub(r'[\u4e00-\u9fa5！？。，《》「」『』－（）【】]', ' ', cleaned_text)
    else:
        cleaned_text = ""
    if "\t" in cleaned_text:
        cleaned_text = cleaned_text.replace("\t", " ")
    cleaned_text = re.sub(r' {2,}', ' ', cleaned_text)

    return cleaned_text


def extract_text(source_fpath):
    doc = fitz.open(source_fpath)
    doc_text = [BeautifulSoup(doc.load_page(pageNum).get_text('text'), 'html.parser').text for pageNum in range(doc.page_count)]

    eng_text_all = []
    chi_text_all = []
    for idx, page in enumerate(doc_text):
        text_list = page.split("\n")
        eng_text_list, chi_text_list, other_text_list = extract_eng_chi(text_list)
        eng_text_all += eng_text_list
        chi_text_all += chi_text_list

    return eng_text_all, chi_text_all


def extract_eng_chi(text_list):
    text_list = [element for element in text_list if len(element) != 0]
    eng_text_list = []
    chi_text_list = []
    other_text_list = []
    for idx, text in enumerate(text_list):
        eng = re.findall(r'[a-zA-Z?!()\[\]{}<>]{1}', text)
        chi = re.findall(r'[\u4e00-\u9fa5！？。，《》「」『』－（）【】]{1}', text)
        other = re.findall(r'[^a-zA-Z\u4e00-\u9fa5]{1}', text)
        if len(eng) == 0 and len(chi) == 0:
            other_text_list.append(text)
        elif (len(eng) - len(chi)) > 0:
            eng_text_list.append(text)
        elif (len(chi) - len(eng)) > 0:
            chi_text_list.append(text)
        else:
            other_text_list.append(text)
    return eng_text_list, chi_text_list, other_text_list


def remove_duplicates(text_list):
    most_common = Counter(text_list).most_common(10)
    top_repeated_text_counts = sorted(list(set([element[1] for element in most_common])), reverse=True)[:2]
    top_repeated_texts = [element[0] for element in most_common if element[1] in top_repeated_text_counts]
    removed = [text for text in text_list if text not in top_repeated_texts]
    return removed


def remove_standalone_text(text_list):
    """
    Finds texts that ends in letters. These are most likely page/section titles, or table titles.
    Example: 'Management Discussion and Analysis'], ['Analysis'], ['Management Discussion and Analysis'],
    ['Overall Performance'], ['Windsor Pavilion, Yantai'], ['Property Rental'], ['Other Operations']
    :param text_list:
    :return:
    """
    removed = [re.findall(r'.*[^a-zA-Z]{1}$', text) for text in text_list]
    flattened_list = [element[0].strip()+" " for element in removed if len(element) != 0]
    return flattened_list


def remove_capitalized_text(text_list):
    capitalized_texts = [text for text in text_list if not re.search(r'[a-z.]', text)]
    removed = [element for element in text_list if element not in capitalized_texts]
    return removed


def main():
    logger.info('Start text extraction')
    script_start_time = time.time()
    utils.init_db_mda_text()
    if not os.path.exists(mda_text_path):
        os.makedirs(mda_text_path)
    get_mda_text(mda_path, saveDoc_text=mda_text_path)
    metadata_db.commit()
    elapsed_time = time.time() - script_start_time
    logger.info(f"MD&A text extraction complete. Elapsed time: {elapsed_time}")
    logger.info('=' * 65)
    return True


if __name__ == "__main__":
    main()