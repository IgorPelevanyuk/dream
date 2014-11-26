from scapy.all import *

SRC_FILE = "eu_fax_frags.cap"
DST_FILE = "pcap.dat"

def putToResults(pck_id):
    seq = sorted(toProcess[pck_id], key = lambda pck: pck.frag)
    data = ''
    for pck in toProcess[pck_id]:
        data += pck.load
    toPickle.append((seq[0].time, seq[0].sprintf("%IP.src%"), data))
    del toProcess[pck_id]

toProcess = {}
toPickle = []

pcap_file = rdpcap(SRC_FILE)
count = 0
for pck in pcap_file:
    count += 1
    if count % 1000 == 0:
        print count
    pck_id = str(pck.id)+str(pck.sprintf("%IP.src%"))
    if pck.flags == 1 and pck.frag == 0L:
        toProcess[pck_id] = [pck]
        continue
    if pck.flags == 1 and pck_id in toProcess:
        toProcess[pck_id].append(pck)
        continue
    if pck.flags == 0 and pck_id in toProcess:
        toProcess[pck_id].append(pck)
        putToResults(pck_id)
        continue
    if pck.flags == 2:
        toPickle.append((pck.time, pck.sprintf("%IP.src%"), pck.load))
print 'ToProcess len: ', len(toProcess)

import pickle
pickle.dump(toPickle, open(DST_FILE, 'w'))
