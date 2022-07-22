"""
This is the main controller of this module.
Last updated: July 2022
"""
from datetime import datetime
import traceback
import sys
import time
from .utils import module_path, download_path, reports_path, mda_path, mda_text_path, metadata_db
from .utils import module_start_time, t1codeVal, t2codeVal, from_date, to_date
from .utils import logger
from . import get_report
from . import get_mda
from . import extract_text


def main():
    try:
        if not get_report.main():
            return
        if not get_mda.main():
            return
        if not extract_text.main():
            return
        elapsed_time = time.time() - module_start_time
        logger.info(f"Module total elapsed time: {elapsed_time}")
        logger.info('=' * 65)
        metadata_db.commit()
        metadata_db.close()
    except Exception:
        logger.debug(traceback.print_exception(*sys.exc_info()))


if __name__ == "__main__":
    main()