from twisted.python import log
from twisted.protocols.memcache import MemCacheProtocol
from twisted.internet import protocol, reactor
import random, time, hashlib, mail, traceback
try:
    import cPickle as pickle
except ImportError:
    log.msg('cPickle not available, using slower pickle library.')
    import pickle

class Cache:
    
    def __init__(self, config):
        self.config = config
        
    def ready(self):
        "Call when the cache is online"
        pass
        
    def set(self, dictionary, time = None):
        "Store value(s) supplied as a python dict for a certain time"
        pass
        
    def get(self, keylist):
        "Retreive a list of values as a python dict"
        return {}
        
class InternalCache(Cache):
    
    def __init__(self, config):
        Cache.__init__(self, config)
        self.cache = {}
        self.ready()
        
    def ready(self):
        limit = self.config.get('memory_limit')
        if not limit:
            log.msg('WARNING: memory_limit not specified, using 100MB as default')
        log.msg("CACHE_BACKEND: Using %s MB in-memory cache" % limit)
        
    def set(self, dictionary, time = None):
        for key, val in dict(dictionary.items()):
            element = {
                'expires_on' : time.time() + (time or 0),
                'element' : val
            }
            dictionary[key] = element
        self.cache.update(element)
            
    def get(self, keylist):
        if not isinstance(keylist, list): keylist = [keylist]
        output = {}
        for key in keylist:
            element = self.cache.get(key)
            if element:
                if time.time() > element['expires_on']:
                    output[key] = None
                else:
                    output[key] = element['element']
        return output
        
    def delete(self, keylist):
        for key in keylist:
            try:
                del self.cache[key]
            except:
                pass
        
    def flush(self):
        self.cache = {}
        
class PythonmemcacheCache(Cache):

    def __init__(self, config):
        Cache.__init__(self, config)
        servers = config['cache_server'].split(',')
        pool_size = int(config.get('cache_pool', 1))
        reactor.suggestThreadPoolSize(500)
        log.msg('Creating memcache connections to servers %s...' % ','.join(servers))
        try:
            import mc
        except:
            log.msg('Failed to import memcache helper library!')
            log.msg(traceback.format_exc())
            return
        try:
            self.mc = mc.Mc(servers)
            self.ready(servers)
        except:
            log.msg('Failed ot create memcache object!')

    def ready(self, servers):
        log.msg("CACHE_BACKEND: Connected to memcache servers %s" % ','.join(servers))

    def set(self, dictionary, time = None):
        "Set all values that are not None"
        pickled_dict = dict([(hashlib.md5(key).hexdigest(), pickle.dumps(val)) for key, val in dictionary.items() if val is not None])
        if len(pickled_dict):
            return self.mc.set_multi(pickled_dict, time)
        else:
            return {}

    def get(self, keylist):
        'Get values'
        if not isinstance(keylist, list): keylist = [keylist]
        md5list = [hashlib.md5(key).hexdigest() for key in keylist]
        return self.mc.get_multi(md5list).addCallback(self._format, keylist, md5list)

    def delete(self, keylist):
        if not isinstance(keylist, list): keylist = [keylist]
        md5list = [hashlib.md5(key).hexdigest() for key in keylist]
        self.mc.delete_multi(md5list)
        
    def _format(self, results, keylist, md5list):
        "Return a dictionary containing all keys in keylist, with cache misses as None"
        output = dict([(key, results.get(md5) and pickle.loads(results[md5])) for key, md5 in zip(keylist, md5list)])
        #log.msg('Memcache results:\n%s' % repr(output))
        return output

    def flush(self):
        log.msg('ERROR: Unsupport operation flush() on PythonMemcacheCache')
        
class MemcacheCache(Cache):

    def __init__(self, config):
        Cache.__init__(self, config)
        server = config['cache_server'].split(',')[0]
        connection_pool_size = int(config.get('cache_pool', 1))
        log.msg('Creating memcache connection pool to server %s...' % server)
        self.pool = []
        try:
            self.host, self.port = server.split(':')
        except:
            self.host = server
            self.port = 11211
        for i in xrange(connection_pool_size):
            d = protocol.ClientCreator(reactor, MemCacheProtocol).connectTCP(self.host, int(self.port))
            d.addCallback(self.ready)

    def ready(self, result=None):
        log.msg("CACHE_BACKEND: Connected to memcache server at %s:%s" % (self.host, self.port))
        self.pool.append(result)
        
    def cache_pool(self):
        return random.choice(self.pool)

    def set(self, dictionary, time = None):
        "Set all values that are not None"
        pickled_dict = dict([(hashlib.md5(key).hexdigest(), pickle.dumps(val)) for key, val in dictionary.items() if val is not None])
        cache = self.cache_pool()
        #log.msg('SET on cache %s' % cache)
        if len(pickled_dict):
            return cache.set_multi(pickled_dict, expireTime = time)
        else:
            return {}

    def get(self, keylist):
        'Get values'
        if not isinstance(keylist, list): keylist = [keylist]
        md5list = [hashlib.md5(key).hexdigest() for key in keylist]
        #log.msg('keylist: %s' % keylist)
        cache = self.cache_pool()
        #log.msg('GET on cache %s' % cache)
        return cache.get_multi(md5list).addCallback(self._format, keylist, md5list)
        
    def delete(self, keylist):
        for key in keylist:
            self.cache_pool().delete(hashlib.md5(key).hexdigest())
        
    def _format(self, results, keylist, md5list):
        "Return a dictionary containing all keys in keylist, with cache misses as None"
        output = dict([(key, results[1].get(md5) and pickle.loads(results[1][md5])) for key, md5 in zip(keylist, md5list)])
        #log.msg('Memcache results:\n%s' % repr(output))
        return output
        
    def flush(self):
        self.cache_pool().flushAll()
        
class NullCache(Cache):

    def __init__(self, config):
        Cache.__init__(self, config)
        self.ready()

    def ready(self):
        log.msg("CACHE_BACKEND: Using NULL cache (Nothing will be cached)")

    def set(self, dictionary, time = None):
        pass

    def get(self, keylist):
        if not isinstance(keylist, list): keylist = [keylist]
        return dict([[key, None] for key in keylist])

    def delete(self, keylist):
        pass
        
    def flush(self):
        pass
        