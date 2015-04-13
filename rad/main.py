import socket
import argparse
import pickle
from pprint import pprint as pp
from select import select 
from decoder import XRDstreamDecoder
from utils import getDomain


import logging
logger = logging.getLogger('newGled')     
hdlr = logging.FileHandler('msg.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)                                          
logger.addHandler(hdlr)                                               
logger.setLevel(logging.INFO)                                        
log = logger

domain = 'dashb-ai-631.cern.ch'
port = int(5163)


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


class XRDStreamUDPListener():
    def __init__(self, domain, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


    def run (self):
        decoder_inst = XRDstreamDecoder()
        counter = 1
        for i in range(0,args.count+1):
            data = pickle.load(open(args.file+str(i)+'.dat'))
            for packet in data:
                counter += 1
                msgs = decoder_inst.decode(packet[0], packet[1], packet[2])
                #pp(msgs)
                if counter % 10000 == 0:
                    print counter
        print 'Done! Processed:', counter

def work():
    XRDStreamUDPListener(domain, port).run()

FOREGROUND = True
def main():
    if FOREGROUND:
        work()
    else:
        pid = TimeoutPIDLockFile(pidfname, 10)
        context = DaemonContext(pidfile=pid)
        context.signal_map = {SIGTERM: stop, SIGINT: stop}
        context.files_preserve = [log.root.handlers[0].stream]
        with context:
            work()

if __name__ == "__main__":
        main()
