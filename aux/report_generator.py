import pickle
import socket
from pprint import pprint as pp
import argparse
import sys
import time
import operator

AMOUNT_OF_PACKS = 38 

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

#following from Python cookbook, #475186
def has_colours(stream):
    if not hasattr(stream, "isatty"):
        return False
    if not stream.isatty():
        return False # auto color only on TTYs
    try:
        import curses
        curses.setupterm()
        return curses.tigetnum("colors") > 2
    except:
        # guess false in case of error
        return False
has_colours = has_colours(sys.stdout)


def printout(text, colour=WHITE):
        if has_colours:
                seq = "\x1b[1;%dm" % (30+colour) + text + "\x1b[0m"
                sys.stdout.write(seq)
        else:
                sys.stdout.write(text)


dns_cache = {}
def getDNSfromIP(ip):
    if ip not in dns_cache:
        try:
            dns_cache[ip] = socket.gethostbyaddr(ip)[0]
        except Exception:
            dns_cache[ip] = 'n/a'
    return dns_cache[ip] 

def getDomain(ip):
    return '.'.join(getDNSfromIP(ip).split('.')[1:])

def pretty_print_distr(distr):
    for domain in distr:
        if 'f' in distr[domain] and 'u' in distr[domain] and '=' in distr[domain]:
            printout(domain + ': ' + str(distr[domain]), GREEN)
        else:
            printout(domain + ': ' + str(distr[domain]), RED)
        print
        print_rate_report(domain)

rates = {}

def add_to_rates(pck, domain):
    if domain not in rates:
        rates[domain] = {'cur_sec': int(pck[0]), 'cur_size':len(pck[2]), 'cur_count':1, 'count':0, 'rate':{}}
        return
    if int(pck[0]) == rates[domain]['cur_sec']:
        rates[domain]['cur_count'] += 1
        rates[domain]['cur_size'] += len(pck[2])
    else:
        rates[domain]['rate'][ rates[domain]['cur_sec'] ] = [ rates[domain]['cur_count'] , rates[domain]['cur_size']]
        rates[domain]['count'] += rates[domain]['cur_count']
        rates[domain]['cur_sec'] = int(pck[0])
        rates[domain]['cur_count'] = 1
        rates[domain]['cur_size'] = len(pck[2])

def rate_calculator(pck, domain):
    add_to_rates(pck, domain)
    add_to_rates(pck, 'ALL')
        
def print_rate_report(domain):
    secs = len(rates[domain]['rate'].keys()) 
    start_time = int(min(rates['ALL']['rate'].keys()))
    end_time = int(max(rates['ALL']['rate'].keys()))
    duration = end_time - start_time
    counts = map(lambda x: x[0], rates[domain]['rate'].values())
    sizes = map(lambda x: x[1], rates[domain]['rate'].values())

    max_count_sec = max(counts)
    max_size_sec = max(sizes)
    avg_count_sec = sum(counts) * 1.0 / duration
    avg_size_sec = sum(sizes) * 1.0 / duration

    print 'Total amount of messages from %s : %s' % (domain, str(rates[domain]['count'])) 
    print 'Total amont of secs during which we received something:', secs, '. Duration of listening: ', duration
    print 'Max size per sec:', max_size_sec
    print 'Max count per sec:', max_count_sec
    print 'Avg size per sec:', avg_size_sec
    print 'Avg count per sec:', avg_count_sec
    print '========================================================'


parser = argparse.ArgumentParser()
parser.add_argument("file", help="File name of dump")
parser.add_argument("count", help="Amount of fragments")
args = parser.parse_args()
args.count = int(args.count)


data = []
for i in range(0,args.count+1):
    cur_data = pickle.load(open(args.file+str(i)+'.dat'))
    
    for pck in cur_data:
        data.append([pck[0], pck[1], pck[2][0]])
        rate_calculator(pck, getDomain(pck[1]))
    print i, 'st file loaded: ', len(data)
        
        
print_rate_report('ALL')
print '======================================================='
printout("Total Packages: ", YELLOW)
printout(str(len(data)), YELLOW)
print
print '--------------------------------------------------------'
print 'First message came:', time.strftime('%d %b %Y %H:%M:%S', time.gmtime(pickle.load(open(args.file+'0.dat'))[0][0]))
tmp = pickle.load(open(args.file+str(args.count)+'.dat'))
print 'Last message came:', time.strftime('%d %b %Y %H:%M:%S', time.gmtime(tmp[len(tmp)-1][0]))
print "The distribution:"
distr = {}
for pck in data:
    domain = getDomain(pck[1])
    if domain not in distr:
        distr[domain] = {}
    if pck[2][0] not in distr[domain]:
        distr[domain][pck[2][0]] = 0
    distr[domain][pck[2][0]] += 1

#pp(distr)
pretty_print_distr(distr)

#import struct
#for pck in data:
#    length = struct.unpack('>cBHI', pck[2][0:8])[2]
#    if length != len(pck[2]):
#        print 'Expected: ', str(length),' Real: ',str(len(pck[2]) )
#        print pck
