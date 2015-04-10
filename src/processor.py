import struct
from utils import Unpack, isBit, getEmptyMessage, getDNSfromIP, getNewUUID, getHexOfInt, getEpoch
from xrootdclasses import *
import calendar
import time
from math import sqrt


import logging
logger = logging.getLogger('newGled')
# hdlr = logging.FileHandler('/root/workspace/myapp.log')
# formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# hdlr.setFormatter(formatter)
# logger.addHandler(hdlr)
log = logger

SEND_UNCLOSED_MESSAGES = False
PERIOD_OF_CHECK = 50
ALLOWED_DELAY = 160

class Processor(object):
    def __init__(self):
        pass

    def processData(self):
        pass

#######################################################################
class XRDstreamProcessor(Processor):
    """
    This class is designed to retrieve info from UDP datagrams and reacts after
    receiving all the necessary information about particular reading/writing
    """
    def __init__(self, addr, reactor):
        super(XRDstreamProcessor, self).__init__()
        self.reactor = reactor
        self.addr = addr
        self.__clearCacheData()
        self.mapper_streams = {'f': self.processFstream,
                               'u': self.processUstream,
                               '=': self.processEQUALstream,
                              }
        self.mapper_f_types = {0: self.read_CLS,
                               1: self.read_OPN,
                               2: self.read_TOD,
                               3: self.read_XFR,
                               4: self.read_DSC
                              }
        self.current_uuid = getNewUUID()
        self.message_counter = 0
        self.STORAGE = {}

    def __clearCacheData(self):
        self.USER_ID = {}
        self.SERVER_INFO = {}

    def __checkCounter(self):
        if self.message_counter > 268435455:     # 268435455 means  0xfffffff
            self.message_counter = 0
            self.current_uuid = getNewUUID()
        log.info("Message counter has been reset")

    def __checkOldMessages(self):
        for fileID in self.STORAGE:
            if (getEpoch() - self.STORAGE[fileID]['update_time']) > ALLOWED_DELAY:
                self.__dealWithOld(fileID)
    
    def __dealWithOld(self, fileID):
        if SEND_UNCLOSED_MESSAGES:
            self.finalizeMessage(fileID)
        else:
            del self.STORAGE[fileID]


    def processData(self, data):
        import pickle
        pickle.dump(data, file('lastData.dat','w'))
        header = Unpack(XrdXrootdMonHeader, data[0:8])
        log.debug("server info: %s", self.SERVER_INFO)
        if (header.stod > self.SERVER_INFO.get('stod')):
            log.debug("stod %s was changed, all cache data for %s removed. New stod is %s", self.SERVER_INFO.get('stod'), self.addr, header.stod)
            self.__clearCacheData()
            self.SERVER_INFO['stod'] = header.stod
        log.debug("Data: %s, Length: %s", str(data), str(len(str(data))))
        log.debug("From addr: %s", self.addr)
        log.debug('code: %s pseq: %s plen: %s stod: %s', header.code, header.pseq, header.plen, header.stod)
        if header.code in self.mapper_streams:
            self.mapper_streams[header.code](data, header)
        else:
            log.warning('Unknown code: %s', header.code)
        log.debug("=====================================================")

    def processFstream(self, data, header):
        current_byte = 8
        log.debug(data)
        highPerf = {}
        while (current_byte < header.plen):
            recType = struct.unpack('>b', data[current_byte:current_byte + 1])[0]
            if recType in self.mapper_f_types:
                if current_byte + 8 <= len(data):
                    expected_length = struct.unpack('>bbhI', data[current_byte:current_byte+8])[2]
                    if (current_byte + expected_length > header.plen) or (current_byte + expected_length > len(data)):
                        print "Datagram corrupted !!!!!!"
                        print "Data length", len(data), ' Def len:', header.plen,' Exp:', expected_length, 'Curr:', current_byte
                        break
                    else:
                        current_byte = self.mapper_f_types[recType](data, current_byte)
                else:
                    print "Datagram corrupted !!!!!!"
                    print "Data length", len(data), ' Def len:', header.plen,' Exp:', expected_length, 'Curr:', current_byte
                    break
            else:
                log.warning("Unrecognized type of F-stream structure: %s", recType)

    def __touchMessage(self, fileID):
        self.STORAGE[fileID]["update_time"] = getEpoch()

    def __updateMessageValue(self, fileID, properties):
        if fileID not in self.STORAGE:
            self.STORAGE[fileID] = getEmptyMessage()
        for (key, value) in properties.items():
            self.STORAGE[fileID][key] = value
        self.__touchMessage(fileID)

    def read_CLS(self, data, current_byte):
        values = {}
        header_CLS = Unpack(XrdXrootdMonFileHdr_CLS, data[current_byte:current_byte + 8])
        current_byte += 8
        log.debug('CLS Flag: %s Size: %s ID: %s', header_CLS.recFlag , header_CLS.recSize , header_CLS.fileID)
        if header_CLS.fileID in self.STORAGE:
            log.debug('ID: %s means: %s', header_CLS.fileID, self.STORAGE[header_CLS.fileID])
        o_XFR = Unpack(XrdXrootdMonStatXFR, data[current_byte:current_byte + 24])
        current_byte += 24
        log.debug('StatXFR read: %s readv: %s write: %s', o_XFR.read, o_XFR.readv, o_XFR.write)
        values = {'read_single_bytes': o_XFR.read,
                   'read_vector_bytes': o_XFR.readv,
                   'write_bytes': o_XFR.write}
        if (isBit(header_CLS.recFlag, 0)):
            log.debug('Forced')
        if (isBit(header_CLS.recFlag, 1)):
            log.debug('Has OPS:')
            o_OPS = Unpack(XrdXrootdMonStatOPS, data[current_byte:current_byte + 48])
            current_byte += 48
            log.debug('Read: %s, Readv: %s, Write: %s', o_OPS.read, o_OPS.readv, o_OPS.write)
            log.debug('rsMin: %s, rsMax: %s, rsegs: %s', o_OPS.rsMin, o_OPS.rsMax, o_OPS.rsegs)
            log.debug('rdMin: %s, rdMax: %s', o_OPS.rdMin, o_OPS.rdMax)
            log.debug('rvMin: %s, rvMax: %s', o_OPS.rvMin, o_OPS.rvMax)
            log.debug('wrMin: %s, wrMax: %s', o_OPS.wrMin, o_OPS.wrMax)
            values.update({
                "read_bytes": o_XFR.read + o_XFR.readv,
                "read_operations": o_OPS.read + o_OPS.readv,
                "read_min": min(o_OPS.read, o_OPS.readv),
                "read_max": max(o_OPS.read, o_OPS.readv),
                "read_average": (o_XFR.read + o_XFR.readv) * 1.0 / (o_OPS.read + o_OPS.readv) if (o_OPS.read + o_OPS.readv) != 0 else 0,
                "read_single_bytes": o_XFR.read,
                "read_single_operations": o_OPS.read,
                "read_single_min": o_OPS.rdMin,
                "read_single_max": o_OPS.rdMax,
                "read_single_average": o_XFR.read * 1.0 / o_OPS.read if o_OPS.read != 0 else 0,
                "read_vector_bytes": o_XFR.readv,
                "read_vector_operations": o_OPS.readv,
                "read_vector_min": o_OPS.rvMin,
                "read_vector_max": o_OPS.rvMax,
                "read_vector_average": o_XFR.readv * 1.0 / o_OPS.readv if o_OPS.readv != 0 else 0,
                "read_vector_count_min": o_OPS.rsMin,
                "read_vector_count_max": o_OPS.rsMax,
                "read_vector_count_average": o_OPS.rsegs / o_OPS.readv if o_OPS.readv != 0 else 0,
                "write_bytes": o_XFR.write,
                "write_operations": o_OPS.write,
                "write_min": o_OPS.wrMin,
                "write_max": o_OPS.wrMax,
                "write_average": o_XFR.write * 1.0 / o_OPS.write if o_OPS.write != 0 else 0,
            })
        if (isBit(header_CLS.recFlag, 2)):
            log.debug('Has SSQ:')
            o_SSQ = Unpack(XrdXrootdMonStatSSQ, data[current_byte:current_byte + 32])
            current_byte += 32
            log.debug('Read: %s, Readv: %s, Rsegs: %s, Wrte: %s', o_SSQ.read, o_SSQ.readv, o_SSQ.rsegs, o_SSQ.write)
            if (o_OPS.read + o_OPS.readv != 0):
                values['read_sigma'] = sqrt((o_SSQ.read + o_SSQ.readv - (o_XFR.read+o_XFR.readv) ** 2 / (o_OPS.read + o_OPS.readv))/(o_OPS.read + o_OPS.readv))
            if (o_OPS.read != 0):
                values['read_single_sigma'] = sqrt((o_SSQ.read - o_XFR.read ** 2 * 1.0 / o_OPS.read) / o_OPS.read)
            if (o_OPS.readv != 0):
                values['read_vector_sigma'] = sqrt((o_SSQ.readv - o_XFR.readv ** 2 * 1.0 / o_OPS.readv) / o_OPS.readv)
                values['read_vector_count_sigma'] = sqrt((o_SSQ.rsegs - o_OPS.rsegs ** 2 * 1.0 / o_OPS.readv) / o_OPS.readv)
            if (o_OPS.write != 0):
                values['write_sigma'] = sqrt((o_SSQ.write - o_XFR.write ** 2 * 1.0 / o_OPS.write) / o_OPS.write)
        values['end_time'] = struct.unpack('>i', data[16:20])[0]
        log.debug(values)
        toPrint = False
        if 'read_bytes' in values:
            values['read_bytes_at_close'] = values['read_bytes']
        else:
            values['read_bytes_at_close'] = values['read_single_bytes'] + values['read_vector_bytes']
            toPrint = True 
        values['write_bytes_at_close'] = values['write_bytes']
        self.__updateMessageValue(header_CLS.fileID, values)
#
#
# Test
        def getUserInfo(u_data):
            userInfo = {}
            print 'udata', u_data
            if '&' not in u_data:
                client_name = u_data.split('@')[1]
                userInfo['h'] = client_name
                return userInfo
            properties = u_data.split('&')
            if '=' not in properties[0]:
                properties.remove(properties[0])
            for prop in properties:
                key = prop.split('=')[0]
                value = prop.split('=')[1]
                userInfo[key] = value
            for k, v in userInfo.items():
                if v == '':
                    del userInfo[k]
            return userInfo

        if toPrint:
            server_name = getDNSfromIP(self.addr)
            server_host = server_name.split('.')[0]
            server_domain = server_name[len(server_host)+1:]
            if 'user_full' in self.STORAGE[header_CLS.fileID] and '&' in self.STORAGE[header_CLS.fileID]['user_full']:
                userInfo = getUserInfo(self.STORAGE[header_CLS.fileID]['user_full'])
                print userInfo
                if 'n' in userInfo:
                    print 'server_username:', userInfo['n']
                else:
                    print 'server_username: n/a'
                if 'o' in userInfo:
                    print 'user_vo:',  userInfo['o']
                else:
                    print 'user_vo: n/a'
                if 'h' in userInfo:
                    client_name = userInfo['h']
                    print 'client_host:', client_name.split('.')[0]
                    print 'client_domain', client_name[len(client_name.split('.')[0])+1:]
                else:
                    print 'client_host n/a'
                    print 'client_domain n/a'
            print 'Server:', server_domain, server_host
            print '============================================================'
# End Test
#
#
        self.finalizeMessage(header_CLS.fileID)
        return current_byte

    def read_OPN(self, data, current_byte):
        values = {}
        header_OPN = Unpack(XrdXrootdMonFileHdr_OPN, data[current_byte:current_byte + 16])
        current_byte += 16
        log.debug('OPN Flag: %s, resSize: %s, ID: %s, fsz: %s', header_OPN.recFlag, header_OPN.recSize, header_OPN.fileID, header_OPN.fsz)
        if (isBit(header_OPN.recFlag, 0)):
            log.debug("Has LFN:")
            user = struct.unpack('>I', data[current_byte:current_byte + 4])[0]
            if user in self.USER_ID:
                log.debug('ID: %s means: %s', user, self.USER_ID[user])
                values['user_full'] = self.USER_ID[user]
            current_byte += 4
            lfn = data[current_byte:current_byte + header_OPN.recSize - 20]
            current_byte += header_OPN.recSize - 20
            values['file_lfn'] = lfn.strip('\x00') # Unknown null bytes in the end
            log.debug("user: %s, lfn: %s", user, lfn)
        if (isBit(header_OPN.recFlag, 1)):
           log.debug('HasRW')
        values['file_size'] = header_OPN.fsz
        values['start_time'] = struct.unpack('>i', data[16:20])[0]
        self.__updateMessageValue(header_OPN.fileID, values)
        return current_byte

    def read_TOD(self, data, current_byte):
        header_TOD = Unpack(XrdXrootdMonFileHdr_TOD, data[current_byte:current_byte + 16])
        current_byte += 16
        log.debug('TOD Flag: %s, resSize: %s, xfrRecs: %s, totalRecs: %s, tBeg: %s, tEnd: %s', header_TOD.recFlag, header_TOD.recSize, header_TOD.xfrRecs, header_TOD.totalRecs, header_TOD.tBeg, header_TOD.tEnd)
        return current_byte

    def read_XFR(self, data, current_byte):
        header_XFR = Unpack(XrdXrootdMonFileHdr_XFR, data[current_byte:current_byte + 8])
        current_byte += 8
        log.debug('XFR Flag: %s Size: %s ID: %s', header_XFR.recFlag , header_XFR.recSize , header_XFR.fileID)
        if header_XFR.fileID in self.STORAGE:
            log.debug('ID: %s means: %s', header_XFR.fileID, self.STORAGE[header_XFR.fileID])
        o_XFR = Unpack(XrdXrootdMonStatXFR, data[current_byte:current_byte + 24])
        current_byte += 24
        log.debug('StatXFR read: %s readv: %s write: %s', o_XFR.read, o_XFR.readv, o_XFR.write)
        values = {'read_single_bytes': o_XFR.read,
                   'read_vector_bytes': o_XFR.readv,
                   'write_bytes': o_XFR.write}
        self.__updateMessageValue(header_XFR.fileID, values)
        return current_byte

    def read_DSC(self, data, current_byte):
        header_DSC = Unpack(XrdXrootdMonFileHdr_DSC, data[current_byte:current_byte + 8])
        current_byte += 8
        log.debug('DSC Flag: %s Size: %s ID: %s', header_DSC.recFlag , header_DSC.recSize , header_DSC.userID)
        if header_DSC.userID in self.USER_ID:
            log.debug('ID: %s means: %s', header_DSC.userID, self.USER_ID[header_DSC.userID])
        return current_byte


    ######################################################################
    #                           Other streams
    ######################################################################
    def processUstream(self, data, header):
        d_dictid = struct.unpack('!I', data[8:12])[0]
        d_user = data[12:len(data)]
        self.USER_ID[d_dictid] = d_user
        log.debug('User info: dictid: %s, user: %s ', d_dictid, d_user)

    def processEQUALstream(self, data, header):
        d_dictid = struct.unpack('!I', data[8:12])[0]
        d_user = data[12:len(data)].split('\n')[0]
        _d_srv_info = data[12:len(data)].split('\n')[1]
        for equation in _d_srv_info.split('&'):
            if ('=' in equation):
                variable, value = equation.split('=')
                self.SERVER_INFO[variable] = value

        log.debug('Server info: dictid: %s, user: %s, srv_info:', d_dictid, d_user)
        for var in self.SERVER_INFO:
            log.debug("%s: %s", var, self.SERVER_INFO[var])

    ######################################################################
    #                           Send functions
    ######################################################################
    def finalizeMessage(self, fileID):

        def getUserInfo(u_data):
            userInfo = {}
            if '&' not in u_data:
                client_hostname= u_data.split('@')[1]
                userInfo['h'] = client_hostname
                return userInfo
            properties = u_data.split('&')
            if '=' not in properties[0]:
                properties.remove(properties[0])
            for prop in properties:
                key = prop.split('=')[0]
                value = prop.split('=')[1]
                userInfo[key] = value
            for k, v in userInfo.items():
                if v == '':
                    del userInfo[k]
            return userInfo

        values = {}
        server_name = getDNSfromIP(self.addr)
        values['server_host'] = server_name.split('.')[0]
        values['server_domain'] = server_name[len(values['server_host'])+1:]
        if 'site' in self.SERVER_INFO:
            values['server_site'] = self.SERVER_INFO['site']
        if 'user_full' in self.STORAGE[fileID] and '&' in self.STORAGE[fileID]['user_full']:
            log.debug("User full: %s", self.STORAGE[fileID]['user_full'])
           
            userInfo = getUserInfo(self.STORAGE[fileID]['user_full'])
            if 'n' in userInfo:
                values['server_username'] = userInfo['n']
            else:
                values['server_username'] = 'n/a'
            if 'o' in userInfo:
                values['user_vo'] = userInfo['o']
            else:
                values['user_vo'] = 'n/a'
            if 'h' in userInfo:
                client_hostname = userInfo['h']
                values['client_host'] = client_hostname.split('.')[0]
                values['client_domain'] = client_hostname[len(values['client_host'])+1:]
            else:
                values['client_host'] = 'n/a'
                values['client_domain'] = 'n/a'
        else:
            log.debug("USER ABSENT")        
        self.__updateMessageValue(fileID, values)
        self.reactor.react(self.STORAGE[fileID])
        del self.STORAGE[fileID]
        log.debug("Message processed")
