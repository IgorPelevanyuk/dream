import pickle

data = pickle.load(file('data.dat.res', 'r'))

import socket
import time
import random

#UDP_IP = "pcipelevan.cern.ch"
#UDP_PORT = 5165
#UDP_IP = "dashb-ai-587"
#UDP_PORT = 9330
UDP_IP = "dashb-ai-631.cern.ch"
UDP_PORT = 5163


print "UDP target IP:", UDP_IP
print "UDP target port:", UDP_PORT

sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP
for i in data:
    sock.sendto(str(i[0]), (UDP_IP, UDP_PORT))
    print 'datasended'
    time.sleep(0.0007)

#sock.sendto(str('finish'), (UDP_IP, UDP_PORT))


