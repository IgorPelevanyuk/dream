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

REQUEST = """
SELECT server_domain, count(*), sum(READ_bytes_at_close), sum(write_bytes_at_close), sum(WRITE_BYTES), sum(READ_BYTES), sum(READ_VECTOR_BYTES) from T_RAW_FED where INSERT_DATE >='#start_d#' and INSERT_DATE < '#end_d#'and queue_flag='eu' and END_TIME>=#start# and END_TIME<#end# group by server_domain
"""

data = json.load(open(JSON_FILE))

parser = argparse.ArgumentParser()
parser.add_argument("-o", "--overview", help="In wich mode application is running", action='store_true')
parser.add_argument("-s", "--start", help="In wich mode application is running")
parser.add_argument("-e", "--end", help="In wich mode application is running")
args = parser.parse_args()

print data[0]
if args.overview:
    #print 'First event end: ', time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(data[0]['end_time']))
    #print 'Last event end: ', time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(data[len(data)-1]['end_time']))
    print 'First event end: ', data[0]['end_time']
    print 'Last event end: ', data[len(data)-1]['end_time']

if args.start and args.end:
    oracle_data = {}
    con = cx_Oracle.connect(CREDENTIALS)
    cur = con.cursor()
    REQUEST = REQUEST.replace('#start#', args.start).replace('#end#',args.end).replace('#start_d#',time.strftime('%d-%b-%Y', time.gmtime(int(args.start)))).replace('#end_d#',time.strftime('%d-%b-%Y', time.gmtime(int(args.end))))
    print REQUEST
    cur.execute(REQUEST)
    for row in cur:
        oracle_data[row[0]] = {}
        oracle_data[row[0]]['count'] = row[1]
        oracle_data[row[0]]['read_close'] = row[2]
        oracle_data[row[0]]['write_close'] = row[3]
        oracle_data[row[0]]['write'] = row[4]
        oracle_data[row[0]]['read'] = row[5]
        oracle_data[row[0]]['read_vector'] = row[6]
    import pprint
    pprint.pprint(oracle_data)
    con.close()
    
    print 'DREAM data'
    dream_data = {}
    for msg in data:
        if msg['server_domain'] not in dream_data:
            dream_data[msg['server_domain']] = {}
            dream_data[msg['server_domain']]['count'] = 0
            dream_data[msg['server_domain']]['read_close'] = 0
            dream_data[msg['server_domain']]['write_close'] = 0
            dream_data[msg['server_domain']]['write'] = 0
            dream_data[msg['server_domain']]['read'] = 0
            dream_data[msg['server_domain']]['read_vector'] = 0
        if msg['end_time'] >= int(args.start) and msg['end_time'] < int(args.end):
            dream_data[msg['server_domain']]['count'] += 1
            dream_data[msg['server_domain']]['read_close'] += msg['read_bytes_at_close']
            dream_data[msg['server_domain']]['write_close'] += msg['write_bytes_at_close']
            dream_data[msg['server_domain']]['write'] += msg['write_bytes']
            dream_data[msg['server_domain']]['read'] += msg['read_bytes']
            dream_data[msg['server_domain']]['read_vector'] += msg['read_vector_bytes']
    pprint.pprint(dream_data)
    print 'Comparison:'
    for domain in oracle_data:
        if domain in dream_data:
            print 'Compare domain: ',domain 
            for prop in oracle_data[domain]:
                print prop, ': ', oracle_data[domain][prop], ' vs. ', dream_data[domain][prop], ' Rate:', 1.0*dream_data[domain][prop]/oracle_data[domain][prop] if oracle_data[domain][prop]!= 0 else 0
	else:
            print 'Domain ', domain, ' is not in the DREAM data'
    for domain in dream_data:
        if domain not in oracle_data:
            print 'Domain ', domain, ' is not in the DREAM data'

