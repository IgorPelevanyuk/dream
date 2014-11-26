from scapy.all import *
import socket
import time
import random

sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP

UDP_IP = "dashb-ai-631.cern.ch"
UDP_PORT = 5164

packetCount = 0


def customAction(packet):
    global packetCount
    global length
    packetCount += 1
    print packetCount
    #sock.sendto(str(packet.load), (UDP_IP, UDP_PORT))


sniff(filter="udp and port 5165",prn=customAction)
#sniff(filter="udp and port 5163",prn=customAction)
