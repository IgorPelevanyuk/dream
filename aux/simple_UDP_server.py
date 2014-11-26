import socket

UDP_IP = "pcipelevan.cern.ch"
UDP_PORT = 5165

sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP
sock.bind((UDP_IP, UDP_PORT))

count = 0
length = 0
receive = True
while receive:
    data, addr = sock.recvfrom(1024) # buffer size is 1024 bytes
    count += 1
    length += len(data)
    print "received message:", data, " \r\n       addr:", addr
    print count
    if (data=='finish'):
        receive = False
        print "%s finished" % str(addr)
        print "received message:", data, " \r\n       addr:", addr
        print count

