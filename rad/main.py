import socket
import argparse
import pickle
import time
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

class XRDStreamUDPListener():
    def __init__(self, domain, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.msgs = []
        self.elapsed_time_read = 0
        self.elapsed_time_write = 0
        self.elapsed_time_process = 0


    def run (self):
        decoder_inst = XRDstreamDecoder()
        counter = 0
        for i in range(0,args.count+1):
            start_time = time.time()
            data = pickle.load(open(args.file+str(i)+'.dat'))
            self.elapsed_time_read += time.time() - start_time
            start_time = time.time()
            for packet in data:
                counter += 1
                msgs = decoder_inst.decode(packet[0], packet[1], packet[2])
                if msgs != []: 
                    self.msgs.append(len(str(msgs)))
#                    self.msgs.append('i')
                if counter % 10000 == 0:
                    print counter
            self.elapsed_time_process += time.time() - start_time
        pp(decoder_inst.codes)
        print 'Done! Processed:', counter
        print 'Decoded:', len(self.msgs)
        print 'Time to read file:', self.elapsed_time_read
        print 'Time to process file:', self.elapsed_time_process
        print 'Total size of result(MB):', sum(self.msgs)*1.0/1000000

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
