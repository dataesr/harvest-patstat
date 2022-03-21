import sys

import pandas as pd
from patstat import collectePatstatComplete, dezippage, p01_family_scope, p02_titles_abstracts, p03_patents, \
    p04_families, p05_creat_participants, p05b_clean_participants, p06_clean_participants_individuals, \
    p07a_get_siren_inpi, p07b_clean_participants_entp, p08_participants_final, p00_outils_inpi_adress, p09_geoloc, \
    comp_version
from application.server.main.logger import get_logger

logger = get_logger(__name__)


def create_task_inpi():
    try:
        ad, nme = p00_outils_inpi_adress.unzip_inpi()
    except:
        ad, nme = pd.DataFrame()
        error = sys.exc_info()[0]
        logger.error(f'Unzipping of INPI files caused an error : {error}')

    return ad, nme


def create_task_harvest_patstat():
    try:
        ls_fl = collectePatstatComplete.main()
        dezippage.main()
    except:
        ls_fl = []
        error = sys.exc_info()[0]
        logger.error(f'Harvest PATSTAT caused an error : {error}')

    return ls_fl


def create_task_p01_p04_patstat():
    try:
        p01_family_scope.get_patentscope()
        print("p01: success")
        p02_titles_abstracts.tit_abst()
        print("p02: success")
        p03_patents.get_pubpat()
        print("p03: success")
        p04_families.getfam()
        print("p04: success")
    except:
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')


def create_task_p05_patstat():
    try:
        p05_creat_participants.start_part()
    except:
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')


def create_task_p05b_patstat():
    try:
        p05b_clean_participants.get_clean_part()
    except:
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')


def create_task_p06_indiv_patstat():
    try:
        p06_clean_participants_individuals.get_clean_ind()
    except:
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')


def create_task_p07_entp_patstat():
    try:
        p07a_get_siren_inpi.get_siren()
        print("p07a: success", flush=True)
        p07b_clean_participants_entp.get_clean_entp()
        print("p07b: success", flush=True)
    except:
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')


def create_task_p08_part_final_patstat():
    try:
        p08_participants_final.part_final()
    except:
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')


def create_json_patent_scanr():
    try:
        comp_version.get_json()
    except:
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')


def create_task_geo():
    try:
        p09_geoloc.geoloc()
    except:
        error = sys.exc_info()[0]
        logger.error(f'Harvest PATSTAT caused an error : {error}')
