import argparse
import pickle


sites = set()
users = set()
server_domains = set()
client_domains = set()

server_reads = {}
server_writes = {}

table = {}

def process_data(data):
  for msg in data:
    if msg['server_site'] != None:
      sites.add(msg['server_site'])
    if msg['server_username'] != 'n/a':
      users.add(msg['server_username'])
    if msg['server_domain'] != 'n/a':
      server_domains.add(msg['server_domain'])
    if msg['client_domain'] != 'n/a':
      client_domains.add(msg['client_domain'])

    if msg['server_site'] not in server_reads:
      server_reads[msg['server_site']] = 0
    if msg['server_site'] not in server_writes:
      server_writes[msg['server_site']] = 0
    server_reads[msg['server_site']] += msg['read_bytes_at_close']
    server_writes[msg['server_site']] += msg['write_bytes_at_close']

    if msg['client_domain'] not in table:
      table[msg['client_domain']] = {}
    if msg['server_site'] not in table[msg['client_domain']]:
      table[msg['client_domain']][msg['server_site']] = 0
    table[msg['client_domain']][msg['server_site']] += msg['read_bytes_at_close']



parser = argparse.ArgumentParser()
parser.add_argument("file", help="File name of dumped messages", nargs='+')
args = parser.parse_args()
print args.file

data = []
for file in args.file:
    data = pickle.load(open(file))
    process_data(data)


for client in table:
    for server in table[client]:
        table[client][server] /= 1000000

from pprint import pprint as pp
pp(users)
pp(sites)
pp(server_domains)
#pp(client_domains)

pp(server_reads)
pp(server_writes)
for i in server_reads:
   print i, ':', server_reads[i]/1000000000000.0, 'TB'

pp(table)
#pp(data[0])
