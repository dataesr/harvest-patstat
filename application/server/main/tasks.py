from patstat.p01_family_scope import get_patent_post2010_appln_id
from application.server.main.logger import get_logger

logger = get_logger(__name__)

def create_task_patstat(args):
    xxxx = args.get('xxx', 1)
    logger.debug(f'launching task with args {args}')

