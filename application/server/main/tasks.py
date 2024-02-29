import os

from patstat import collectePatstatCompleteBDDS, dezippage, p01_family_scope, p02_titles_abstracts, p03_patents, \
    p04_families, p05_creat_participants, p05b_clean_participants, p06_clean_participants_individuals, \
    p07a_get_siren_inpi, p07b_clean_participants_entp, p08_participants_final, p00_outils_inpi_adress, \
    comp_version, ftp_inpi, p000_lib_cpc, p06b_collecteIdRef, files_dataesr , recreate_partfin_recuperation, p07c, \
    p08_participants_final_copy, loading_mongo, get_doi_from_npl, get_patent_from_doi
# comp_version_y02y04s,
from application.server.main.logger import get_logger

logger = get_logger(__name__)

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def create_task_doi(args):
    if args.get("liste"):
        if isinstance(args.get("liste"), list):
            liste = args.get("liste")
            dict_pat = get_patent_from_doi.get_patents_common_doi(liste)
            print(dict_pat[0], flush=True)
        else:
            logger.debug("Objet n'est pas une liste")
    else:
        logger.debug("Liste pas présente dans les arguments")
    return dict_pat


def create_task_clean(args):
    os.system(f'rm -rf {DATA_PATH}/data_PATSTAT_Global_*')
    os.system(f'rm -rf {DATA_PATH}/index_documentation_scripts_PATSTAT*')
    os.system(f'rm -rf {DATA_PATH}/tls*')


def create_task_all(args):
    # if args.get('recuperation', True):
    #     create_recuperation()
    if args.get('harvest_inpi', True):
        harvest_inpi()
    if args.get('unzip_inpi', True):
        create_task_inpi()
    if args.get('harvest_patstat', True):
        create_task_harvest_patstat()
    if args.get('lib_cpc', True):
        create_lib_cpc()
    if args.get('p01_p04', True):
        create_task_p01_p04_patstat()
    if args.get('rec', True):
        create_recuperation()
    if args.get('p05', True):
        create_task_p05_patstat()
    if args.get('p05b', True):
        create_task_p05b_patstat()
    if args.get('p06', True):
        create_task_p06_indiv_patstat()
    if args.get('p07', True):
        create_task_p07_entp_patstat()
    if args.get('p07c', True):
        create_task_p07c()
    if args.get('p08', True):
        create_task_p08_part_final_patstat()
    # if args.get('geo', True):
    #    create_task_geo()
    if args.get('export_scanr', True):
        create_json_patent_scanr()
    if args.get('create_dataesr', True):
        create_dataesr()
    if args.get('create_mongo', True):
        create_loading_mongo()
    if args.get('create_doi', True):
        get_doi()


def harvest_inpi():
    ftp_inpi.loading()
    logger.debug("chargement de la dernière version complète de la DB de l'INPI")


def create_task_inpi():
    p00_outils_inpi_adress.unzip_inpi()
    logger.debug("p00: success")


def create_task_harvest_patstat():
    logger.debug("début create task harvest patstat")
    collectePatstatCompleteBDDS.harvest_patstat()
    logger.debug("Collecte PATSTAT complete : success")
    logger.debug("Début dezippage")
    dezippage.unzip()
    logger.debug("dezippage : success")


def create_task_p01_p04_patstat():
    p01_family_scope.get_patentscope()
    logger.debug("p01: success")
    p02_titles_abstracts.tit_abst()
    logger.debug("p02: success")
    p03_patents.get_pubpat()
    logger.debug("p03: success")
    p04_families.getfam()
    logger.debug("p04: success")


def create_task_p05_patstat():
    p05_creat_participants.start_part()
    logger.debug("p05: success")


def create_task_p05b_patstat():
    p05b_clean_participants.get_clean_part()
    logger.debug("p05b: success")


def create_task_p06_indiv_patstat():
    p06_clean_participants_individuals.get_clean_ind()
    logger.debug("p06: success")
    p06b_collecteIdRef.collecte()
    logger.debug("p06b: success")


def create_task_p07_entp_patstat():
    p07a_get_siren_inpi.get_siren()
    logger.debug("p07a: success")
    p07b_clean_participants_entp.get_clean_entp()
    logger.debug("p07b: success")


def create_task_p07c():
    p07c.siren_oeb_bodacc()
    logger.debug("p07c: success")


def create_task_p08_part_final_patstat():
    p08_participants_final.part_final()
    logger.debug("p08: success")


def create_json_patent_scanr():
    comp_version.get_json()
    logger.debug("final json scanr created.")
    # comp_version_y02y04s.get_json_env()


def create_lib_cpc():
    p000_lib_cpc.lib_cpc()
    logger.debug("file CPC codes created.")


def create_dataesr():
    files_dataesr.get_dataesr()
    logger.debug("files dataESR created.")


def create_recuperation():
    recreate_partfin_recuperation.recreate()
    logger.debug("Fichier old_part recupéré")


def create_loading_mongo():
    loading_mongo.load_data()
    logger.debug("Tables chargées dans mongo")

def get_doi():
    get_doi_from_npl.load_pbln_doi_to_mongo()
    logger.debug("DOI chargées dans mongo")

# def create_task_geo():
#    p09_geoloc.geoloc()
#    logger.debug("p09: success")
