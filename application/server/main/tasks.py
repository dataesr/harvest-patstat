import sys

import pandas as pd
from patstat import collectePatstatComplete, dezippage, p01_family_scope, p02_titles_abstracts, p03_patents, \
    p04_families, p05_creat_participants, clean_participants, p06_clean_participants_individuals, \
    p07a_get_siren_inpi, p07b_clean_participants_entp, p08_participants_final, outils_inpi_adress, p09_geoloc
from application.server.main.logger import get_logger

logger = get_logger(__name__)


def create_task_inpi():
    try:
        ad, nme = outils_inpi_adress.main()
    except:
        ad, nme = pd.DataFrame()
        error = sys.exc_info()[0]
        logger.error(f'Unzipping of INPI files caused an error : {error}')

    return ad, nme


def create_task_harvest_patstat():
    try:
        ls_fl = collectePatstatComplete.main()
        df_dezip = dezippage.main()
    except:
        ls_fl = []
        df_dezip = pd.DataFrame()
        error = sys.exc_info()[0]
        logger.error(f'Harvest PATSTAT caused an error : {error}')

    return ls_fl, df_dezip


def create_task_process_patstat():
    try:
        ptt_scope = p01_family_scope.main()
        ttles, abs = p02_titles_abstracts.main()
        pub, pat = p03_patents.main()
        fam, fam_tech_codes = p04_families.main()
        part_init2, part_init3 = p05_creat_participants.main()
        part_ind, part_entp = clean_participants.main()
        sex_table, part_ind = p06_clean_participants_individuals.main()
        siren_inpi_brevet, siren_inpi_generale = p07a_get_siren_inpi.main()
        part_entp_final = p07b_clean_participants_entp.main()
        particip, participants, idext, role = p08_participants_final.main()
    except:
        ptt_scope, ttles, abs, pub, pat, fam, fam_tech_codes, part_init2, part_init3, part_ind, part_entp, \
        sex_table, siren_inpi_brevet, siren_inpi_generale, part_entp_final, particip, participants, idext, \
        role = pd.DataFrame()
        error = sys.exc_info()[0]
        logger.error(f'Process PATSTAT caused an error : {error}')

    return ptt_scope, ttles, abs, pub, pat, fam, fam_tech_codes, part_init2, part_init3, part_ind, part_entp, \
           sex_table, siren_inpi_brevet, siren_inpi_generale, part_entp_final, particip, participants, idext, role


def create_task_geo():
    try:
        best_scores_b, best_geocod = p09_geoloc.main()
    except:
        best_scores_b, best_geocod = pd.DataFrame()
        error = sys.exc_info()[0]
        logger.error(f'Harvest PATSTAT caused an error : {error}')

    return best_scores_b, best_geocod
