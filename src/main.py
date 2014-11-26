import socket
from select import select 
from processor import XRDstreamProcessor
from reactor import FileReactor, PickleReactor
from utils import getDomain

import logging
logger = logging.getLogger('newGled')     
hdlr = logging.FileHandler('msg.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)                                          
logger.addHandler(hdlr)                                               
logger.setLevel(logging.WARNING)                                        
log = logger

domain = 'dashb-ai-631.cern.ch'
port = int(5163)

class XRDStreamUDPListener():
    def __init__(self, domain, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((domain, port))
        self.processors = {}
        self.reactor = PickleReactor("msg.dat")

    def run (self):
        # ticker = 0
        # while ticker >= 0:
        #     ticker += 1
        #     inputready, outputready, exceptready = select([self.sock], [], [])
        #     for sock_inst in inputready:
        #         if sock_inst == self.sock:
        #             data, addr = sock_inst.recvfrom(int(10000))
        #         if data:
        #             self.__assignData(data, addr[0])
        #     log.info(ticker)
        # self.sock.close()
        # log.info('exit XRDStreamUDPListener')

        import pickle
        data = pickle.load(file('pcap.dat', 'r'))
        counter = 1
        for packet in data:
            counter += 1
            if getDomain(packet[1])=='icepp.jp':
                self.__assignData(packet[2], packet[1])
            if counter % 1000 == 0:
                print counter
        self.reactor.finalize()


    def __assignData(self, data, addr):
        if addr in self.processors:
            self.processors[addr].processData(data)
        else:
            self.processors[addr] = XRDstreamProcessor(addr, self.reactor)
            self.processors[addr].processData(data)
            # TODO: put reactor

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
