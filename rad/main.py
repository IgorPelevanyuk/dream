import socket
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

class XRDStreamUDPListener():
    def __init__(self, domain, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


    def run (self):
        decoder_inst = XRDstreamDecoder()
        import pickle
        data = pickle.load(file('/afs/cern.ch/user/i/ipelevan/pcap0.dat', 'r'))
        counter = 1
        for packet in data:
            counter += 1
            msgs = decoder_inst.decode(packet[0], packet[1], packet[2])
            #print packet[0], packet[2], packet[1]
            from pprint import pprint as pp
            #pp(msgs)
            if counter % 1000 == 0:
                print counter
        print counter
        pickle.dump(decoder_inst.uid, file('temp.dat', 'w'))

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
