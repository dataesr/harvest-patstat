import sys

import pandas as pd
from patstat import collectePatstatComplete, dezippage, p01_family_scope, p02_titles_abstracts, p03_patents, \
    p04_families, p05_creat_participants, p05b_clean_participants, p06_clean_participants_individuals, \
    p07a_get_siren_inpi, p07b_clean_participants_entp, p08_participants_final, p00_outils_inpi_adress, p09_geoloc, \
    comp_version
from application.server.main.logger import get_logger

logger = get_logger(__name__)

def create_task_all(args):
    if args.get('harvest_inpi', True):
        # TODO !
        pass
    if args.get('unzip_inpi', True):
        create_task_inpi()
    if args.get('harvest_patstat', True):
        create_task_harvest_patstat()
    if args.get('p01_p04', True):
        create_task_p01_p04_patstat()
    if args.get('p05', True):
        create_task_p05_patstat()
    if args.get('p05b', True):
        create_task_p05b_patstat()
    if args.get('p06', True):
        create_task_p06_indiv_patstat()
    if args.get('p07', True):
        create_task_p07_entp_patstat()
    if args.get('p08', True):
        create_task_p08_part_final_patstat()
    if args.get('geo', True):
        create_task_geo()
    if args.get('export_scanr', True):
        create_json_patent_scanr()

def create_task_inpi():
    try:
        ad, nme = p00_outils_inpi_adress.unzip_inpi()
        print("p00: success", flush=True)
    except:
        ad, nme = pd.DataFrame()
        error = sys.exc_info()[0]
        logger.error(f'Unzipping of INPI files caused an error : {error}')

    return ad, nme


def create_task_harvest_patstat():
    try:
        ls_fl = collectePatstatComplete.main()
        print("Collecte PATSTAT complete : success", flush=True)
        dezippage.main()
        print("dezippage : success", flush=True)
    except:
        ls_fl = []
        error = sys.exc_info()[0]
        logger.error(f'Harvest PATSTAT caused an error : {error}')

    return ls_fl


def create_task_p01_p04_patstat():
    try:
        p01_family_scope.get_patentscope()
        print("p01: success", flush=True)
        p02_titles_abstracts.tit_abst()
        print("p02: success", flush=True)
        p03_patents.get_pubpat()
        print("p03: success", flush=True)
        p04_families.getfam()
        print("p04: success", flush=True)
    except:
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')


def create_task_p05_patstat():
    try:
        p05_creat_participants.start_part()
        print("p05: success", flush=True)
    except:
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')


def create_task_p05b_patstat():
    try:
        p05b_clean_participants.get_clean_part()
        print("p05b: success", flush=True)
    except:
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')


def create_task_p06_indiv_patstat():
    try:
        p06_clean_participants_individuals.get_clean_ind()
        print("p06: success", flush=True)
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
        print("p08: success", flush=True)
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
        print("p09: success", flush=True)
    except:
        error = sys.exc_info()[0]
        logger.error(f'Harvest PATSTAT caused an error : {error}')
