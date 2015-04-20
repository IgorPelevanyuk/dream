from ctypes import *

######################################################################
#                           Packet Header
######################################################################
class XrdXrootdMonHeader(BigEndianStructure):
    _fields_ = [("code", c_char),
                ("pseq", c_uint8), # c_char
                ("plen", c_int16),
                ("stod", c_int32)]

######################################################################
#                           F-Stream Headers
######################################################################
class XrdXrootdMonFileHdr_CLS(BigEndianStructure):  # recType 0; 7b
    _fields_ = [("recType", c_int8),
                ("recFlag", c_int8),
                ("recSize", c_int16),
                ("fileID", c_uint32)]  #WARNING: could be wronly named


class XrdXrootdMonFileHdr_OPN(BigEndianStructure):  # recType 1; 15b
    _fields_ = [("recType", c_int8),
                ("recFlag", c_int8),
                ("recSize", c_int16),
                ("fileID", c_uint32),  #WARNING: could be wronly named
                ("fsz", c_int64)]

class XrdXrootdMonFileHdr_TOD(BigEndianStructure):  # recType 2; 15b
    _fields_ = [("recType", c_int8),
                ("recFlag", c_int8),
                ("recSize", c_int16),
                ("xfrRecs", c_int16),
                ("totalRecs", c_int16),
                ("tBeg", c_int32),
                ("tEnd", c_int32)]

class XrdXrootdMonFileHdr_XFR(BigEndianStructure):  # recType 3; 7b
    _fields_ = [("recType", c_int8),
                ("recFlag", c_int8),
                ("recSize", c_int16),
                ("fileID", c_uint32)]  #WARNING: could be wronly named

class XrdXrootdMonFileHdr_DSC(BigEndianStructure):  # recType 4; 7b
    _fields_ = [("recType", c_int8),
                ("recFlag", c_int8),
                ("recSize", c_int16),
                ("userID", c_uint32)]  #WARNING: could be wronly named


######################################################################
#                      F-Stream Specific Structures
######################################################################
class XrdXrootdMonStatXFR(BigEndianStructure):  # 24b
    _fields_ = [("read", c_int64),
                ("readv", c_int64),
                ("write", c_int64)]

class XrdXrootdMonStatOPS(BigEndianStructure):  # 48b
    _fields_ = [("read", c_int32),
                ("readv", c_int32),
                ("write", c_int32),
                ("rsMin", c_int16),
                ("rsMax", c_int16),
                ("rsegs", c_int64),
                ("rdMin", c_int32),
                ("rdMax", c_int32),
                ("rvMin", c_int32),
                ("rvMax", c_int32),
                ("wrMin", c_int32),
                ("wrMax", c_int32)]

class XrdXrootdMonStatSSQ(BigEndianStructure):  #32b
    _fields_ = [("read", c_int64),
                ("readv", c_int64),
                ("rsegs", c_int64),
                ("write", c_int64)]

######################################################################
#                      T-Stream Specific Structures
######################################################################
class XROOTD_MON_OPEN(BigEndianStructure):
    _fields_ = [("id", c_ubyte * 8),
                ("notused", c_uint32),
                ("dictid", c_uint32)]

class XROOTD_MON_APPID(BigEndianStructure):
    _fields_ = [("id", c_ubyte * 8)]

class XROOTD_MON_CLOSE(BigEndianStructure):
    _fields_ = [("id", c_ubyte * 4),
                ("rTot", c_uint32),
                ("wTot", c_uint32),
                ("dictid", c_uint32)]

class XROOTD_MON_DISC(BigEndianStructure):
    _fields_ = [("id", c_ubyte * 8),
                ("buflen", c_int32),
                ("dictid", c_uint32)]

class XROOTD_MON_WINDOW(BigEndianStructure):
    _fields_ = [("id", c_ubyte * 8),
                ("prev_ended", c_int32),
                ("this_started", c_int32)]

class XROOTD_RW_REQUEST(BigEndianStructure):
    _fields_ = [("val", c_uint64),
                ("buflen", c_int32),
                ("dictid", c_uint32)]

class XROOTD_MON_READV(BigEndianStructure):
    _fields_ = [("val", c_ubyte),
                ("vector", c_ubyte),
                ("sval", c_int16),
                ("rtot", c_uint32),
                ("buflen", c_int32),
                ("dictid", c_uint32)]