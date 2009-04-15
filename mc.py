from twisted.internet import defer, threads
from twisted.python import threadable, log
threadable.init(1)
import traceback, sys, StringIO

class Mc:
    
    def __init__(self, urls):
        self.connection = False
        try:
            import memcache
            self.enabled = True
        except ImportError:
            log.msg("Error - memcache bindings not installed!")
            self.enabled = False
        if self.enabled:
            self.connection = memcache.Client(urls, debug=1)
                        
    def set(self, key, value, time=0):
        return threads.deferToThread(self.doSet, key, value, time)
        
    def set_multi(self, mapping, time=0):
        return threads.deferToThread(self.doSetMulti, mapping, time)
        
    def delete(self, key):
        return threads.deferToThread(self.doDelete, key)

    def delete_multi(self, keys):
        return threads.deferToThread(self.doDeleteMulti, keys)
        
    def get(self, key):
        return threads.deferToThread(self.doGet, key)
        
    def get_multi(self, keys):
        return threads.deferToThread(self.doGetMulti, keys)
        
    def increment(self, key):
        return threads.deferToThread(self.doIncr, key)

    def decrement(self, key):
        return threads.deferToThread(self.doDecr, key)
                
    def doSet(self, key, value, time):
        if self.enabled: self.connection.set(key, value, time)
        return True
        
    def doSetMulti(self, mapping, time):
        if self.enabled: self.connection.set_multi(mapping, time)
        return True
        
    def doDelete(self, key):
        if self.enabled: self.connection.delete(key)
        return True

    def doDeleteMulti(self, keys):
        if self.enabled: self.connection.delete_multi(keys)
        return True
        
    def doGet(self, key):
        if self.enabled:
            value = self.connection.get(key)
        else:
            return False
        return value
        
    def doGetMulti(self, keys):
        if self.enabled: values = self.connection.get_multi(keys)
        else: return False
        return values
        
    def doIncr(self, key):
        if self.enabled: self.connection.incr(key)
        return True
    def doDecr(self, key):
        if self.enabled: self.connection.decr(key)
        return True
