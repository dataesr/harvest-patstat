import sys

from patstat import collectePatstatComplete, dezippage, p01_family_scope, p02_titles_abstracts, p03_patents, \
    p04_families, p05_creat_participants, clean_participants, p06_clean_participants_individuals, \
    p07a_get_siren_inpi, p07b_clean_participants_entp, p08_participants_final, outils_inpi_adress, p09_geoloc
from application.server.main.logger import get_logger

logger = get_logger(__name__)


def create_task_inpi():
    try:
        res = outils_inpi_adress()
    except:
        error = sys.exc_info()[0]
        logger.error(f'Unzipping of INPI files caused an error : {error}')

    return res.ad, res.name


def create_task_harvest_patstat():
    try:
        collectePatstatComplete()
        dezippage()
    except:
        error = sys.exc_info()[0]
        logger.error(f'Harvest PATSTAT caused an error : {error}')


def create_task_process_patstat():
    try:
        p01_family_scope()
        p02_titles_abstracts()
        p03_patents()
        p04_families()
        p05_creat_participants()
        clean_participants()
        p06_clean_participants_individuals()
        p07a_get_siren_inpi()
        p07b_clean_participants_entp()
        p08_participants_final()
    except:
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')


def create_task_geo():
    try:
        p09_geoloc()
    except:
        error = sys.exc_info()[0]
        logger.error(f'Harvest PATSTAT caused an error : {error}')


def create_task_patstat():
    try:
        create_task_inpi()
        create_task_harvest_patstat()
        create_task_process_patstat()
        create_task_geo()
    except:
        error = sys.exc_info()[0]
        logger.error(f'PATSTAT all caused an error : {error}')
