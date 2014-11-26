from ctypes import *
import calendar
import time
import socket
import uuid


def isBit(val, idx):
    return ((val & (1 << idx)) != 0);

def Unpack(ctype, buf):
    cstring = create_string_buffer(buf)
    ctype_instance = cast(pointer(cstring), POINTER(ctype)).contents
    return ctype_instance

def getEmptyMessage():
    emptyMessage = {
        "update_time": getEpoch(),
        "unique_id": None,
        "file_lfn": 'n/a',
        "file_size": 0,
        "start_time": 0, 
        "end_time": 0,
        "read_bytes": 0,
        "read_operations": 0,
        "read_min": 0,
        "read_max": 0,
        "read_average": 0,
        "read_sigma": 0,
        "read_single_bytes": 0,
        "read_single_operations": 0,
        "read_single_min": 0,
        "read_single_max": 0,
        "read_single_average": 0,
        "read_single_sigma": 0,
        "read_vector_bytes": 0,
        "read_vector_operations": 0,
        "read_vector_min": 0,
        "read_vector_max": 0,
        "read_vector_average": 0,
        "read_vector_sigma": 0,
        "read_vector_count_min": 0,
        "read_vector_count_max": 0,
        "read_vector_count_average": 0,
        "read_vector_count_sigma": 0,
        "write_bytes": 0,
        "write_operations": 0,
        "write_min": 0,
        "write_max": 0,
        "write_average": 0,
        "write_sigma": 0,
        "read_bytes_at_close": 0,
        "write_bytes_at_close": 0,
        "user_dn": 'n/a',
        "user_vo": 'n/a',
        "user_role": 'n/a',
        "user_fqan": 'n/a',
        "client_domain": 'n/a',
        "client_host": 'n/a',
        "server_username": 'n/a',
        "app_info": 'n/a',
        "server_domain": 'n/a',
        "server_host": 'n/a',
        "server_site": None
    }
    return emptyMessage

#TODO: DNS cache cleaning
#TODO: Smart warning, no silent failures
dns_cache = {}
def getDNSfromIP(ip):
    if ip not in dns_cache:
        try:
            dns_cache[ip] = socket.gethostbyaddr(ip)[0]
        except Exception:
            dns_cache[ip] = 'n/a'
    return dns_cache[ip] 

def getDomain(ip):
    return '.'.join(getDNSfromIP(ip).split('.')[1:])

def getIP():
    return socket.gethostbyname(socket.gethostname())

def getIntIP():
    ip = getIP().split('.')
    ip = [int(x) for x in ip]
    ip = (ip[0]<<24) + (ip[1]<<16) + (ip[2]<<8) + ip[3]
    return ip

def getHexIP():
    return "%x" % getIntIP()

def getNewUUID():
    return str(uuid.uuid1())

def getHexOfInt(val, rec_length = 7):
    result = "%x" % val
    result = (rec_length-len(result))*'0' + result
    return result

def getEpoch():
    return calendar.timegm(time.gmtime())