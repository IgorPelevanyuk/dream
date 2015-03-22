import pickle
import argparse
import sys
import time

AMOUNT_OF_PACKS = 13
DIR = '/root/days/'

parser = argparse.ArgumentParser()
parser.add_argument("file", help="File name of dump")
args = parser.parse_args()

data = []
current_day = ''
current_data = []

for i in range(0,AMOUNT_OF_PACKS):
    data = pickle.load(open(args.file+str(i)+'.dat'))
    print i, 'st file loaded: ', len(data)
    for pck in data:
        if time.strftime('%b%d', time.gmtime(data[0][0])) == current_day:
            current_data.append(pck)
        else:
            if current_day != '':
                pickle.dump(current_data, open(DIR + current_day + '.dat', 'w'))
            current_day = time.strftime('%b%d', time.gmtime(data[0][0]))
            current_data = [pck]
pickle.dump(current_data, open(DIR + current_day + '.dat', 'w'))    
 
