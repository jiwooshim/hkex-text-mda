# HKEX Text MD&A
**hkex-text-mda** crawls, downloads, and extracts the "Management Discussion and Analysis" seciton from the PDF files. The extracted section are saved in both PDF and TXT formats for usage suitable for further PDF or text processing. 

Includes all reports data from HKEX from 2007 June onwards, as allowed in https://www.hkexnews.hk/. Outputs organized structure of files in separate sub-directories for an easy accessibility, includes a log and a SQLite3 database file for further processing (like failure analysis or creating statistics summary). 

## Installation 
Clone this repository, install requirements, and run as a module
```
git clone git@github.com:jiwooshim/hkex-text-mda.git
pip install -r requirements.txt
```

## Usage
### Input arguments
| Parameter | Required | Description |
| -- | -- | -- |
|-dp --download_path | False | Absolute directory where you want to save your file. <br/>Default:.reports_data |
| -t2 --report_type | True | -2 => All Financial Statements/ESG Information, <br/>40100 => Annual Report, <br/>40200 => Semi-annual Report <br/>40300 => Quarterly Report <br/>40400 => ESG Information/Report |
| -fd --from_date | True | From date of the report to be donwloaded. <br/>Format='yyyymmdd' for days, 'yyyymm' for months, and 'yyyy' for years. For example, '2018' will include all reports from 2018 to the year specified in --to_date. <br/>NOTICE: The format of --from_date and --to_date should match. <br/>NOTICE2: The earliest date value limit is 2007 June. |
| -td --to_date | True | To date of the report to be downloaded. <br/>Format='yyyymmdd' for days, 'yyyymm' for months, and 'yyyy' for years. For example, '2018' will include all reports up to 2018 from the year specified in --from_date. <br/>NOTICE: The format of --from_date and --to_date should match |

### Example
Download annual reports from Jan. 2020 to Jun. 2021, in the specified path at /home/jiwooshim/hkex_reports directory.
```
python3 -m hkex-text-mda -t2 40100 -fd 202001 -td 202106 -dp /home/jiwooshim/hkex_reports
```

### Output
This module yields three main outputs plus two others.

#### 1. "hkex_reports" directory 
* Contains original PDF files organized by months. 
* File format: ```{STOCK-CODE}\_{NEWS-ID}\_{yyyymmdd}\_{REPORT-TITLE-HYPHENED}.pdf```

#### 2. "hkex_reports_mda" directory
* Contains extracted PDF files for MD&A organized by months. 
* File format: ```{STOCK-CODE}\_{NEWS-ID}\_{yyyymmdd}\_{REPORT-TITLE-HYPHENED}\_pages_{START-PAGE}-{END-PAGE}\_{MD&A-OUTLINE-TITLE-MATCHED}.pdf```

#### 3. "hkex_reports_mda_text" directory
* Contains extracted TXT files for MD&A organized by months. 
* File format: ```{STOCK-CODE}\_{NEWS-ID}\_{yyyymmdd}\_{REPORT-TITLE-HYPHENED}\_pages_{START-PAGE}-{END-PAGE}\_{MD&A-OUTLINE-TITLE-MATCHED}\_{ENG-OR-CHI}.txt```

#### 4. "metadata_yyyymmdd.db" database
* Contains three tables each containing report details for each of the above outputs.

#### 5. "hkex-text.log" logfile
* Logger set-up for logging the whole operation.

## Success rate
The overall success rate is 81.5% for all reports, including those originally coming without an MD&A such as ETF reports. Also, MD&A can sometimes be included in sections in a different name, like "Chairman's Statements" (especially for older reports). The success rate that includes the keyword "Chairman's Statements" is 93.3%, but this module avoids it to keep the most accurate extraction. 

