"""
This is a module for performing a comparioson of monitoring results from GLED
and DREAMx
"""
import cx_Oracle
from pprint import pprint as pp
import time
import json
import argparse

CREDENTIALS = 'atlas_xrd_mon_r/xrdmonatl8r!@LCGR'
JSON_FILE = 'msg.json'

data = json.load(open(JSON_FILE))

parser = argparse.ArgumentParser()
parser.add_argument("-o", "-owerview", help="In wich mode application is running")


if parser.owerview:
	print parser