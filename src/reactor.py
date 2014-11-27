import uuid
from utils import getHexOfInt


import logging
logger = logging.getLogger('newGled')
# hdlr = logging.FileHandler('/root/workspace/myapp.log')
# formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# hdlr.setFormatter(formatter)
# logger.addHandler(hdlr)
log = logger

SCHEMA = {
    "update_time": 0,
    "unique_id": 'str',
    "file_lfn": 'str',
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
    "user_dn": 'str',
    "user_vo": 'str',
    "user_role": 'str',
    "user_fqan": 'str',
    "user_full": 'str',
    "client_domain": 'str',
    "client_host": 'str',
    "server_username": 'str',
    "app_info": 'str',
    "server_domain": 'str',
    "server_host": 'str',
    "server_site": 'str'
}

class Reactor(object):
    def __init__(self):
        self.message_counter = 0
        self.current_uuid = str(uuid.uuid1())

    def getUniqueID(self):
        self.message_counter += 1
        self.__checkCounter()
        return self.current_uuid + getHexOfInt(self.message_counter)

    def react(self):
        pass

    def __checkCounter(self):
        if self.message_counter > 268435455:     # 268435455 means  0xfffffff
            self.message_counter = 0
            self.current_uuid = str(uuid.uuid1())
        log.info("Message counter has been reset")


class FileReactor(Reactor):
    def __init__(self, filePath):
        super(FileReactor, self).__init__()
        self.filePath = filePath
        self.messages = []

    def react(self, message):
        message['unique_id'] = self.getUniqueID()
        self.messages.append(message)

    def finalize(self):
        fo = open(self.filePath, "w")
        fo.write('[')        
        for msg in self.messages:
            fo.write('{\n')
            sorted_keys = sorted(msg.keys())
            for key in sorted_keys:
                if SCHEMA[key] == 'str':
                    fo.write('   "%s": "%s"'%(key, msg[key]))
                else:
                    fo.write('   "%s": %s'%(key, msg[key]))
                if key != sorted_keys[-1]:
                    fo.write(",\n")
            fo.write('}')
            if msg != self.messages[-1]:
                    fo.write(",\n")
        fo.write(']')
        fo.close()
        
class PickleReactor(Reactor):
    def __init__(self, filePath):
        super(PickleReactor, self).__init__()
        self.filePath = filePath
        self.messages = []

    def react(self, message):
        message['unique_id'] = self.getUniqueID()
        self.messages.append(message)

    def finalize(self):
        import pickle
        pickle.dump(self.messages, file(self.filePath, 'w'))