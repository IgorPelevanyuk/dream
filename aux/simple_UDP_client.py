import socket
import time
import random

UDP_IP = "dashb-ai-631.cern.ch"
#UDP_IP = "127.0.0.1"
UDP_PORT = 5164
MESSAGE = "Hello, World!"

print "UDP target IP:", UDP_IP
print "UDP target port:", UDP_PORT
print "message:", MESSAGE

sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP
data =  ''
for i in range(0,1000):
    data += str(i) + '.'

tail = 0
while (data != 42):
    #data = random.randint(0,100)
    print len(str(data)[0:1470+tail%30])
    print str(data)[0:1470+tail%30]
    sock.sendto(str(data)[0:1470+tail%30], (UDP_IP, UDP_PORT))
    tail += 1
    time.sleep(2)


