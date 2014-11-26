import socket
import time
import pickle
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("host", help="Host name of destination")
parser.add_argument("port", help="Port number of destination ", type=int)
parser.add_argument("-d", "--delay", help="Delay in miliseconds", type=int)

args = parser.parse_args()

print "UDP target IP:", args.host
print "UDP target port:", args.port
if args.delay:
    print 'Delay: ', args.delay, ' ms'

sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP

data = pickle.load(open('day.dat'))

count = 0
for pck in data:
    ip = [chr(int(ip_part)) for ip_part in pck[1].split('.')]
    sock.sendto(''.join(ip)+str(pck[2]), (args.host, args.port))
    print 'Data sent. IP: ', pck[1], ', data length: ', len(pck[2])
    if args.delay:
        time.sleep(args.delay * 1.0 / 1000)

