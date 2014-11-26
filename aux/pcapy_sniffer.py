import pcapy
from impacket.ImpactDecoder import *
 
# list all the network devices
print pcapy.findalldevs()
  
max_bytes = 10000
promiscuous = False
read_timeout = 0 # in milliseconds
pc = pcapy.open_live("eth0", max_bytes, promiscuous, read_timeout)
    
pc.setfilter('udp and port 5164')
     
# callback for received packets
countPacket = 0
def recv_pkts(hdr, data):
    global countPacket
    countPacket += 1
    packet = EthDecoder().decode(data)
    print len(data)
    print countPacket
    #print packet
     
packet_limit = -1 # infinite
pc.loop(packet_limit, recv_pkts) # capture packets
