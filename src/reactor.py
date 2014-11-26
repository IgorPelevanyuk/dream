import uuid
from utils import getHexOfInt


import logging
logger = logging.getLogger('newGled')
# hdlr = logging.FileHandler('/root/workspace/myapp.log')
# formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# hdlr.setFormatter(formatter)
# logger.addHandler(hdlr)
log = logger

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

    def react(self, message):
        fo = open(self.filePath, "a")
        fo.write('{\n')
        for key in sorted(message.keys()):
            fo.write('   %s: %s,\n'%(key, message[key]))
        fo.write('},\n')
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