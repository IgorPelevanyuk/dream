from scapy.all import *
import pickle

SRC_FILE = "/root/fax_eu_dump_II.cap"
DST_FILE = "/root/pcap"

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
glued = 0
file_index = 0
for pck in pcap_file:
    count += 1
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
        glued += 1
        continue
    if pck.flags == 2:
        toPickle.append((pck.time, pck.sprintf("%IP.src%"), pck.load))
    if count % 10000 == 0:
        print count, ': toPickle ', len(toPickle),'; glued:', glued
        if len(toPickle) > 249999:
            pickle.dump(toPickle, open(DST_FILE+str(file_index)+'.dat', 'w'))
            print 'File pickled'
            file_index += 1
            toPickle = []
  
print 'ToProcess len: ', len(toProcess)
pickle.dump(toPickle, open(DST_FILE+str(file_index)+'.dat', 'w'))

print 'task done'
