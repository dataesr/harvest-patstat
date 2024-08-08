#!/usr/bin/env python
# coding: utf-8

import os

from utils import swift

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

def fun1():
    swift.download_object('patstat', 'part_p08.csv', f'{DATA_PATH}part_p08.csv')
    swift.download_object('patstat', 'part_p05.csv', f'{DATA_PATH}part_p05.csv')
    swift.download_object('patstat', 'part_init_p05.csv', f'{DATA_PATH}part_init_p05.csv')