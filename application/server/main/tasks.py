import sys

from patstat import collectePatstatComplete, dezippage, p01_family_scope, p02_titles_abstracts, p03_patents, \
    p04_families, p05_creat_participants, clean_participants, p06_clean_participants_individuals, \
    p07a_get_siren_inpi, p07b_clean_participants_entp, p08_participants_final, utils_inpi_adress, p09_geoloc
from application.server.main.logger import get_logger

logger = get_logger(__name__)


def create_task_patstat(args: dict) -> None:
    logger.debug(f'Creating task patstat with args {args}')
    task = args.get('task')
    if task == 'harvest_inpi':
        outils_inpi_adress()
    elif task == 'harvest_patstat':
        collectePatstatComplete()
        dezippage()
    elif task == 'cleaning_patstat':
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
    elif task == 'geoloc':
        p09_geoloc()
    elif task == 'all':
        outils_inpi_adress()
        try:
            collectePatstatComplete()
        except:
            error = sys.exc_info()[0]
            logger.error(f'Harvesting of PATSTAT caused an error : {error}')
        dezippage()
        p01_family_scope()
        p02_titles_abstracts()
        p03_patents()
        p04_families()
        p05_creat_participants()
        clean_participants()
        try:
            p06_clean_participants_individuals()
        except:
            error = sys.exc_info()[0]
            logger.error(f'Gender got all confused and caused an error : {error}')
        p07a_get_siren_inpi()
        p07b_clean_participants_entp()
        p08_participants_final()
        try:
            p09_geoloc
        except:
            error = sys.exc_info()[0]
            logger.error(f'Geoloc caused an error : {error}')
    else:
        logger.error(
            'Task error: your request should have a task between "harvest_inpi", '
            '"harvest_patstat", "cleaning_patstat", "geoloc" or "all".')
