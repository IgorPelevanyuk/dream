import pickle
import socket
from pprint import pprint as pp
import argparse
import sys
import time

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

parser = argparse.ArgumentParser()
parser.add_argument("file", help="File name of dump")
args = parser.parse_args()

data = 0
if '.json' in args.file:
    import json
    data = json.load(open(args.file))
else:
    data = pickle.load(open(args.file))
print '======================================================='
printout("Total Packages: ", YELLOW)
printout(str(len(data)), YELLOW)
print
print '--------------------------------------------------------'
print 'First message came:', time.strftime('%d %b %Y %H:%M:%S', time.gmtime(data[0][0]))
print 'Last message came:', time.strftime('%d %b %Y %H:%M:%S', time.gmtime(data[len(data)-1][0]))
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

import struct
for pck in data:
    length = struct.unpack('>cBHI', pck[2][0:8])[2]
    if length != len(pck[2]):
        print 'Expected: ', str(length),' Real: ',str(len(pck[2]) )
        print pck
