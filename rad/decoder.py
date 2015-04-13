import struct
from utils import Unpack, isBit, getEmptyMessage, getDNSfromIP, getNewUUID, getHexOfInt, getEpoch
from xrootdclasses import *
import calendar
import time
from math import sqrt

from pprint import pprint as pp
import logging
logger = logging.getLogger('newGled')
# hdlr = logging.FileHandler('/root/workspace/myapp.log')
# formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# hdlr.setFormatter(formatter)
# logger.addHandler(hdlr)
log = logger


class Decoder(object):
    def __init__(self):
        pass

    def decode(self):
        pass

#######################################################################
class XRDstreamDecoder(Decoder):
    """
    This class is designed to decode info from UDP datagrams. 
    """
    def __init__(self):
        super(XRDstreamDecoder, self).__init__()
        self.mapper_streams = {'f': self.processFstream,
                               'u': self.processUstream,
                               '=': self.processEQUALstream,
                               'd': self.processDstream,
                               'i': self.processIstream,
                               't': self.processTstream,
                               'r': self.processRstream
                              }
        self.mapper_f_types = {0: self.read_CLS,
                               1: self.read_OPN,
                               2: self.read_TOD,
                               3: self.read_XFR,
                               4: self.read_DSC
                              }
        self.current_msgs = []
        self.message_counter = 0
 
        self.uid = []

    def decode(self, time, addr, data):
        
        #import pickle
        #pickle.dump(data, file('lastData.dat','w'))
        xrd_header = Unpack(XrdXrootdMonHeader, data[0:8])
        header = {} 
	header['h_time'] = time
        header['h_sender'] = addr
        header['h_code'] = xrd_header.code
        header['h_pseq'] = xrd_header.pseq
        header['h_plen'] = xrd_header.plen
        header['h_stod'] = xrd_header.stod
        if xrd_header.code in self.mapper_streams:
            self.mapper_streams[xrd_header.code](data, header)
        else:
            print xrd_header.code
        #    log.warning('Unknown code: %s', xrd_header.code)
        return self.current_msgs
        log.debug("=====================================================")

    def processFstream(self, data, header):
        current_byte = 8
        log.debug(data)
        while (current_byte < header['h_plen']):
            recType = struct.unpack('>b', data[current_byte:current_byte + 1])[0]
            if recType in self.mapper_f_types:
                if current_byte + 8 <= len(data):
                    expected_length = struct.unpack('>bbhI', data[current_byte:current_byte+8])[2]
                    if (current_byte + expected_length > header['h_plen']) or (current_byte + expected_length > len(data)):
                        log.warning("Datagram corrupted ! Data length: %s. Def len: %s. Exp: %s. Current byte: %s." %s (str(len(data)), header['h_plen'], expected_length, current_byte))
                        break
                    else:
                        current_byte = self.mapper_f_types[recType](data, current_byte, header)
                else:
                    log.warning("Datagram corrupted !Data length: %s. Def len: %s. Exp: %s. Curr: %s" % (len(data), header['h_plen'], expected_length, current_byte))
                    break
            else:
                log.warning("Unrecognized type of F-stream structure: %s", recType)
    
    def read_CLS(self, data, current_byte, header):
        values = {'recType': 'CLS'}
        header_CLS = Unpack(XrdXrootdMonFileHdr_CLS, data[current_byte:current_byte + 8])
        values['recFlag'] = header_CLS.recFlag
        values['recSize'] = header_CLS.recSize
        values['fileId'] = header_CLS.fileID
        log.debug('CLS Flag: %s Size: %s ID: %s', header_CLS.recFlag , header_CLS.recSize , header_CLS.fileID)
        current_byte += 8
        
        o_XFR = Unpack(XrdXrootdMonStatXFR, data[current_byte:current_byte + 24])
        values['xfr_read'] = o_XFR.read
        values['xfr_readv'] = o_XFR.readv
        values['xfr_write'] = o_XFR.write
        log.debug('StatXFR read: %s readv: %s write: %s', o_XFR.read, o_XFR.readv, o_XFR.write)
        current_byte += 24
        
        values['isForced'] = False
        if (isBit(header_CLS.recFlag, 0)):
            values['forced'] = True

        values['isOps'] = False
        if (isBit(header_CLS.recFlag, 1)):
            log.debug('Has OPS:')
            o_OPS = Unpack(XrdXrootdMonStatOPS, data[current_byte:current_byte + 48])
            values['isOps'] = True
            values['read'] = o_OPS.read
            values['readv'] = o_OPS.readv
            values['write'] = o_OPS.write
            values['rsMin'] = o_OPS.rsMin
            values['rsMax'] = o_OPS.rsMax
            values['rsegs'] = o_OPS.rsegs
            values['rdMin'] = o_OPS.rdMin
            values['rdMax'] = o_OPS.rdMax
            values['rvMin'] = o_OPS.rvMin
            values['rvMax'] = o_OPS.rvMax
            values['wrMin'] = o_OPS.wrMin
            values['wrMax'] =o_OPS.wrMax

            current_byte += 48
            log.debug('Ops Read: %s, Readv: %s, Write: %s', o_OPS.read, o_OPS.readv, o_OPS.write)
            log.debug('rsMin: %s, rsMax: %s, rsegs: %s', o_OPS.rsMin, o_OPS.rsMax, o_OPS.rsegs)
            log.debug('rdMin: %s, rdMax: %s', o_OPS.rdMin, o_OPS.rdMax)
            log.debug('rvMin: %s, rvMax: %s', o_OPS.rvMin, o_OPS.rvMax)
            log.debug('wrMin: %s, wrMax: %s', o_OPS.wrMin, o_OPS.wrMax)

        values['isSSQ'] = False
        if (isBit(header_CLS.recFlag, 2)):
            log.debug('Has SSQ:')
            o_SSQ = Unpack(XrdXrootdMonStatSSQ, data[current_byte:current_byte + 32])
            values['isSSQ'] = True
            values['ssqRead'] = o_SSQ.read
            values['ssqReadv'] = o_SSQ.readv
            values['ssqRsegs'] = o_SSQ.rsegs
            values['ssqWrite'] = o_SSQ.write

            current_byte += 32
            log.debug('Read: %s, Readv: %s, Rsegs: %s, Wrte: %s', o_SSQ.read, o_SSQ.readv, o_SSQ.rsegs, o_SSQ.write)
        values['header'] = header
        self.current_msgs.append(values)
        return current_byte

    def read_OPN(self, data, current_byte, header):
        values = {'recType': 'OPN'}
        header_OPN = Unpack(XrdXrootdMonFileHdr_OPN, data[current_byte:current_byte + 16])
        values['recFlag'] = header_OPN.recFlag
        values['recSize'] = header_OPN.recSize
        values['fileId'] = header_OPN.fileID
        values['fileSize'] = header_OPN.fsz
        current_byte += 16
        log.debug('OPN Flag: %s, resSize: %s, ID: %s, fsz: %s', header_OPN.recFlag, header_OPN.recSize, header_OPN.fileID, header_OPN.fsz)

        values['isLFN'] = False
        if (isBit(header_OPN.recFlag, 0)):
            values['isLFN'] = True
            log.debug("Has LFN:")
            user = struct.unpack('>I', data[current_byte:current_byte + 4])[0]
            values['userId'] = user
            current_byte += 4
            
            lfn = data[current_byte:current_byte + header_OPN.recSize - 20]
            current_byte += header_OPN.recSize - 20
            values['file_lfn'] = lfn.strip('\x00') # Unknown null bytes in the end
            log.debug("user: %s, lfn: %s", user, lfn)
        values['hasRW'] = False
        if (isBit(header_OPN.recFlag, 1)):
           values['hasRW'] = True
           log.debug('HasRW')
        values['header'] = header
        self.current_msgs.append(values)
        return current_byte

    def read_TOD(self, data, current_byte, header):
        values = {'recType': 'TOD'}
        header_TOD = Unpack(XrdXrootdMonFileHdr_TOD, data[current_byte:current_byte + 16])
        values['recFlag'] = header_TOD.recFlag
        values['recSize'] = header_TOD.recSize
        values['xfrRecs'] = header_TOD.xfrRecs
        values['totalRecs'] = header_TOD.totalRecs
        values['tBeg'] = header_TOD.tBeg
        values['tEnd'] = header_TOD.tEnd

        current_byte += 16
        log.debug('TOD Flag: %s, resSize: %s, xfrRecs: %s, totalRecs: %s, tBeg: %s, tEnd: %s', header_TOD.recFlag, header_TOD.recSize, header_TOD.xfrRecs, header_TOD.totalRecs, header_TOD.tBeg, header_TOD.tEnd)

        values['header'] = header
        self.current_msgs.append(values)
        return current_byte

    def read_XFR(self, data, current_byte, header):
        values = {'recType': 'XFR'}
        header_XFR = Unpack(XrdXrootdMonFileHdr_XFR, data[current_byte:current_byte + 8])
        values['recFlag'] = header_XFR.recFlag
        values['recSize'] = header_XFR.recSize
        values['fileId'] = header_XFR.fileID
        current_byte += 8
        log.debug('XFR Flag: %s Size: %s ID: %s', header_XFR.recFlag , header_XFR.recSize , header_XFR.fileID)
    
        o_XFR = Unpack(XrdXrootdMonStatXFR, data[current_byte:current_byte + 24])
        values['xfr_read'] = o_XFR.read
        values['xfr_readv'] = o_XFR.readv
        values['xfr_write'] = o_XFR.write
        current_byte += 24
        log.debug('StatXFR read: %s readv: %s write: %s', o_XFR.read, o_XFR.readv, o_XFR.write)
        values['header'] = header
        self.current_msgs.append(values)
        return current_byte

    def read_DSC(self, data, current_byte, header):
        values = {'recType': 'DSC'}
        header_DSC = Unpack(XrdXrootdMonFileHdr_DSC, data[current_byte:current_byte + 8])
        values['recFlag'] = header_DSC.recFlag
        values['recSize'] = header_DSC.recSize
        values['userId'] = header_DSC.userID
        current_byte += 8
        log.debug('DSC Flag: %s Size: %s ID: %s', header_DSC.recFlag , header_DSC.recSize , header_DSC.userID)
        values['header'] = header
        self.current_msgs.append(values)
        return current_byte


    ######################################################################
    #                           Other streams
    ######################################################################
    def processUstream(self, data, header):
        values = {}
        userId = struct.unpack('!I', data[8:12])[0]
        values['userId'] = userId
       
        u_data = data[12:len(data)]
        
        if '&' not in u_data:
            values['h'] = u_data.split('@')[1]
            return 
        properties = u_data.split('&')
        if '=' not in properties[0]:
            properties.remove(properties[0])
        for prop in properties:
            key = prop.split('=')[0]
            value = prop.split('=')[1]
            values[key] = value
        for k, v in values.items():
            if v == '':
                del values[k]
        
        from pprint import pprint as pp
        #pp(u_data)
        #pp(values)
        return values

    def processEQUALstream(self, data, header):
        values = {'header': header}
        d_dictid = struct.unpack('!I', data[8:12])[0]
        d_host = data[12:len(data)].split('\n')[0]
        _d_srv_info = data[12:len(data)].split('\n')[1]
        for equation in _d_srv_info.split('&'):
            if ('=' in equation):
                key, value = equation.split('=')
                values[key] = value
        values['host'] = d_host
        log.debug('Server info: dictid: %s, user: %s, srv_info:', d_dictid, d_host)
        return values

    def processDstream(self, data, header):
        values = {'header': header}
        d_dictid = struct.unpack('!I', data[8:12])[0]
        d_host = data[12:len(data)].split('\n')[0]
        d_file =  data[12:len(data)].split('\n')[1]
        values['fileid'] = d_dictid
        values['host'] = d_host
        values['file'] = d_file
        return values

    def processIstream(self, data, header):
        pass

    def processTstream(self, data, header):
        pass

    def processRstream(self, data, header):
        pass
