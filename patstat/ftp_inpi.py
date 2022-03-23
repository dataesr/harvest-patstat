#!/usr/bin/env python
# coding: utf-8

import ftplib
import re

from patstat import config

ftp_server = ftplib.FTP(config.HOSTNAME_INPI, config.USERNAME_INPI, config.PWD_INPI)

liste = []
ftp_server.retrlines('LIST', liste.append)

ftp_server.quit()

liste2 = liste

split = [item.rsplit(None, 1)[-1] for item in liste2]

year_dir = [re.findall(r"^\d{4}$", item) for item in split]
year_dir = [item for item in year_dir if item]