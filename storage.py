from twisted.internet import reactor, protocol, defer
from twisted.enterprise import adbapi
from twisted.protocols.memcache import MemCacheProtocol, DEFAULT_PORT
from twisted.python import log
import traceback, urllib, time
import cache, http, mail
import random # for ab testing
import cPickle as pickle

class DataStore:
    # Status codes
    uncacheable_status = [500, 502, 503, 504, 304, 307]
    uncacheable_methods = ['POST', 'PUT', 'DELETE']
    short_status = [404]
    
    def __init__(self, config):
        self.config = config   

        # Mecache Backend
        servers = config.get('backend_memcache').split(',')
        log.msg('Creating connections to backend_memcache servers %s...' % ','.join(servers))
        try:
            import mc
        except:
            log.msg('Failed to import memcache helper library!')
            log.msg(traceback.format_exc())
            return
        try:
            self.proto = mc.Mc(servers)
            log.msg('backend_memcache OK')
        except:
            log.msg('Failed ot create memcache object!')
            log.msg(traceback.format_exc())
            
        # Viewdb Backend
        servers = config.get('backend_viewdb').split(',')
        log.msg('Creating connections to backend_viewdb servers %s...' % ','.join(servers))
        try:
            import mc
        except:
            log.msg('Failed to import memcache helper library!')
            log.msg(traceback.format_exc())
            return
        try:
            self.viewdb = mc.Mc(servers)
            log.msg('backend_viewdb OK')
        except:
            log.msg('Failed ot create memcache object!')
            log.msg(traceback.format_exc())

        # Database Backend
        try:
            self.db = adbapi.ConnectionPool("pyPgSQL.PgSQL", 
                database=config['backend_dbname'], 
                host=config['backend_dbhost'], 
                user=config['backend_dbuser'], 
                password=config['backend_dbpass'],
                cp_noisy=True,
                cp_reconnect=True,
                cp_min=5,
                cp_max=20,
            )
            log.msg("Connected to db.")
        except ImportError:
            mail.error("Could not import PyPgSQL!\n%s" % traceback.format_exc())
        except:
            mail.error("Unable to connect to backend database.\n%s" % traceback.format_exc())
        
        # AB Testing Groups
        self.abTestingGroups = {}
        self.loadAbTestingGroups()
        
         # HTTP Backend
        try:
            self.backend_host, self.backend_port = self.config['backend_webserver'].split(':')
            self.backend_port = int(self.backend_port)
        except:
            self.backend_host = self.config['backend_webserver']
            self.backend_port = 80
            
        # Cache Backend
        log.msg('Initializing cache...')
        cache_type = config['cache_type'].capitalize() + 'Cache'
        self.cache = getattr(cache, cache_type)(config)     
        
        # Memorize variants of a uri
        self.uri_lookup = {}

        # Request pileup queue
        self.pending_requests = {}
    
    # Init status

    def dbConnected(self, db):
        log.msg('Database connection success.')
        self.db = db
                    
    # Main methods

    def get(self, keys, request, force=False):
        if not isinstance(keys, list): keys = [keys]
        if force:
            d = self.handleMisses(dict(zip(keys, [None for key in keys])), request)
        else:
            d = defer.maybeDeferred(self.cache.get, keys)
            d.addCallback(self.handleMisses, request)
            d.addErrback(self.getError)
        return d
        
    def delete(self, keys):
        if not isinstance(keys, list): keys = [keys]
        self.cache.delete(keys)
        
    def flush(self):
        "Flush entire cache"
        self.cache.flush()

    def handleMisses(self, dictionary, request):
        "Process hits, check for validity, and fetch misses / invalids"
        missing_deferreds = []
        missing_elements = []
        for key, value in dictionary.items():
            fetch = False
            if value is None:
                log.msg('MISS [%s]' % key)
                fetch = True
            elif not getattr(self, 'valid_' + self.elementType(key))(request, self.elementId(key), value):
                log.msg('INVALID [%s]' % key)
                fetch = True
            else:
                log.msg('HIT [%s]' % key)
            if fetch:
                d = defer.maybeDeferred(getattr(self, 'fetch_' + self.elementType(key)), request, self.elementId(key))
                d.addErrback(self.fetchError, key)
                missing_deferreds.append(d)
                missing_elements.append(key)
        # Wait for all items to be fetched
        if missing_deferreds:
            deferredList = defer.DeferredList(missing_deferreds)
            deferredList.addCallback(self.returnElements, dictionary, missing_elements)
            return deferredList
        else:
            return defer.succeed(dictionary)
        
    def returnElements(self, results, dictionary, missing_elements):
        if not isinstance(results, list): results = [results]
        uncached_elements = dict([(key, results.pop(0)[1]) for key in missing_elements])
        dictionary.update(uncached_elements)
        return dictionary

    def fetchError(self, result, key):
        log.msg('Error calling fetch_%s for key %s' % (self.elementType(key), self.elementId(key)))
        log.msg(result.getErrorMessage().replace('\n', ' '))
        result.printTraceback()
        return {}

    def getError(self, dictionary):
        log.msg('uh oh! %s' % dictionary)
        traceback.print_exc()

    # Hashing
            
    def elementHash(self, request, element_type, element_id = None):
        "Hash function for elements"
        return getattr(self, 'hash_' + element_type.lower())(request, element_id)
    
    # elementType's are not allowed to have _ in them because we use those in composing ids
    def elementType(self, key):
        return key.split('_')[0]
        
    def elementId(self, key):
        return '_'.join(key.split('_')[1:])
        
    # Expirations

    def hash_expiration(self, request, id):
        return 'expiration_' + request.uri.rstrip("?")

    def fetch_expiration(self, request, id):
        return False

    def valid_expiration(self, request, id, value):
        return True
                
    # Page
    
    def hash_page(self, request, id=None, cookies = [], abdependency = [], abvalue = {}):
        # Hash the request key
        key = 'page_' + (request.getHeader('x-real-host') or request.getHeader('host') or '') + request.uri.rstrip("?")
        # Internationalization salt
        if self.config.get('hash_lang_header'):
            header = request.getHeader('accept-language') or self.config.get('hash_lang_default', 'en-us')
            if header:
                try:
                    lang = header.replace(' ', '').split(';')[0].split(',')[0].lower()
                    #log.msg('lang: %s' % lang)
                    key += '//' + lang
                except:
                    traceback.print_exc()

        if abdependency:
            ab_identifier = ','.join(':'.join([key, abvalue.get(key, '')]) for key in abdependency)
            key += '//' + ab_identifier

        if cookies:
            # Grab the cookies we care about from the request
            found_cookies = []
            for cookie in cookies:
                val = request.getCookie(cookie)
                if val:
                    found_cookies.append('%s=%s' % (cookie, val))
            # Update key based on cookies we care about
            if found_cookies:
                key += '//' + ','.join(found_cookies)
        log.msg("HASHED PAGE %s" % key)
        return key
        
    def fetch_page(self, request, id, ignoreResult=False):
        # Prevent idental request pileup
        key = self.hash_page(request)
        if key in self.pending_requests and ignoreResult:
            log.msg('PENDING: Request is already pending for %s' % request.uri)
            return True
        # Tell backend that we are Twice and strip cache-control headers
        request.setHeader(self.config.get('twice_header'), 'true')
        request.removeHeader('cache-control')
        # Make the request
        sender = http.HTTPRequestSender(request)
        #sender.noisy = False
        reactor.connectTCP(self.backend_host, self.backend_port, sender)
        # Defer the result 
        d = sender.deferred.addCallback(self.extract_page, request).addErrback(self.page_failed, request)
        self.pending_requests[key] = d
        return d
        
    def valid_page(self, request, id, value):
        "Determine whether the page can be served from the cache"
        # Force refetch of very stale (3x cache_control value) pages
        now = time.time()
        if now > value['rendered_on'] + value['cache_control'] * 3:
            log.msg('STALE-HARD [%s]' % id)
            return False
        # Sevre semi-stale pages but refresh in the background
        elif now > value['rendered_on'] + value['cache_control']:
            log.msg('STALE-SOFT [%s]' % id)

            # Extend the valid cache length by 30s so we can fetch it
            response = value['response']
            cookies = sorted((response.getHeader(self.config.get('cookies_header')) or '').split(','))
            key = self.hash_page(request, cookies = cookies)
            cache_control = response.getCacheControlHeader(self.config.get('cache_header')) or 0
            if response.status in self.uncacheable_status:
                log.msg('NO-CACHE (Status is %s) [%s]' % (response.status, key))
                cache = False
            elif response.status in self.short_status:
                log.msg('SHORT-CACHE (Status is %s) [%s]' % (response.status, key))
                cache = True
                cache_control = 30
            elif cache_control and cache_control > 0:
                log.msg('CACHE [%s] (for %ss)' % (key, cache_control))
                cache = True
            else:
                log.msg('NO-CACHE (No cache data) [%s]' % key)
                cache = False
            value['rendered_on'] += 30
            if cache:
                self.cache.set({key : value}, 60) # Give 60s to refresh the page

            # Now fetch a fresh copy in the background
            self.fetch_page(request, id, ignoreResult=True)
            return True
        # Do not serve cached versions of pages if the method is not cacheable
        elif request.method.upper() in self.uncacheable_methods:
            log.msg('PASS-THROUGH [%s]' % request.method.upper())
            return False
        # Valid page
        else:
            return True
        
    def page_failed(self, response, request):
        # Release pending lock
        key = self.hash_page(request)
        if key in self.pending_requests:
            del self.pending_requests[key]
        
        log.msg('ERROR: Could not retrieve [%s]' % request.uri.rstrip("?h"))
        response.printBriefTraceback()
        # TODO: Return something meaningful!
        return ''
        
    def extract_page(self, response, request):
        # Release pending lock
        key = self.hash_page(request)
        if key in self.pending_requests:
            del self.pending_requests[key]
        
        # Extract uniqueness info
        cookies = sorted((response.getHeader(self.config.get('cookies_header')) or '').split(','))

        # Extract AB test dependencies
        abvalue_string = request.getHeader(self.config.get('abvalue_header'))
        if abvalue_string:
            abvalue = dict(value.split(':') for value in abvalue_string.split(','))
        else:
            abvalue = {}
        abdependency = sorted((response.getHeader(self.config.get('abdependency_header')) or '').split(','))

        key = self.hash_page(request, cookies = cookies, abdependency = abdependency, abvalue = abvalue)

        # Store uri variant
        if key not in self.uri_lookup.setdefault(request.uri.rstrip("?"), []):
            log.msg('Added new variant for %s: %s' % (request.uri.rstrip("?"), key))
            self.uri_lookup[request.uri.rstrip("?")].append(key)

        # Override for non GET's
        if request.method.upper() in self.uncacheable_methods:
            log.msg('NO-CACHE (Method is %s) [%s]' % (request.method, key))
            cache = False
            cache_control = 0
        else:    
            # Cache logic
            cache_control = response.getCacheControlHeader(self.config.get('cache_header')) or 0
            if response.status in self.uncacheable_status:
                log.msg('NO-CACHE (Status is %s) [%s]' % (response.status, key))
                cache = False
            elif response.status in self.short_status:
                log.msg('SHORT-CACHE (Status is %s) [%s]' % (response.status, key))
                cache = True
                cache_control = 30
            elif cache_control and cache_control > 0:
                log.msg('CACHE [%s] (for %ss)' % (key, cache_control))
                cache = True
            else:
                log.msg('NO-CACHE (No cache data) [%s]' % key)
                cache = False

        # Actual return value  
        value =  {
            'response' : response,
            'rendered_on' : time.time(),
            'cache_control' : cache_control,
        }
        if cache:
            # save page
            response.cookies = []
            self.cache.set({key : value}, cache_control * 10) # 10x cache control length
        if abdependency:
            # save abdependency
            abdependency_key = self.hash_abdependency(request)
            self.cache.set({abdependency_key: abdependency}, cache_control * 10)
        return value
    
    # Memcache
    
    def hash_memcache(self, request, id):
        return 'memcache_' + id
    
    def fetch_memcache(self, request, id):
        #log.msg('Looking up memcache %s' % id)
        return self.proto.get(id).addCallback(self.extract_memcache, request, id)  
        
    def extract_memcache(self, result, request, id):
        # Un-comment for the twisted memcached library
        #value = result and result[1]
        value = result
        key = self.hash_memcache(request, id)
        self.cache.set({key: value}, 30) # 30 seconds
        return value
        
    def valid_memcache(self, request, id, value):
        return True
                
    def incr_memcache(self, key):
        log.msg('Incrementing memcache %s' % key)
        return self.proto.increment(key)

    def decr_memcache(self, key):
        log.msg('Decrementing memcache %s' % key)
        return self.proto.decrement(key)
        
    def delete_memcache(self, key):
        self.cache.delete(key)
        return self.proto.delete(key)
        
    def set_memcache(self, key, val):
        log.msg('Setting memcache %s' % key)
        return self.proto.set(key, val)
        
    # Viewdb
    
    def hash_viewdb(self, request, id):
        return 'viewdb_' + id
    
    def fetch_viewdb(self, request, key):
        #log.msg("Looking up viewdb %s" % key)
        return self.viewdb.get(key).addCallback(self.extract_viewdb, request, key)
   
    def extract_viewdb(self, result, request, id):
        # Un-comment for the twisted memcached library
        #value = result and result[1]
        value = result
        key = self.hash_viewdb(request, id)
        self.cache.set({key: value}, 30) # 30 seconds
        #log.msg('Set twice cache key %s as %s' % (key, value))
        return value
        
    def valid_viewdb(self, request, id, value):
        return True
        
    def incr_viewdb(self, key):
        log.msg('Incrementing viewdb %s' % key)
        self.viewdb.add(key, "0")
        return self.viewdb.increment(key)

    def set_viewdb(self, key, val):
        log.msg('Setting viewdb %s' % key)
        return self.viewdb.add(key, val)
    
    # viewdb for unread message counts per user
    def fetch_unread(self, request, id):
        id = self._read_session(request)
        key = "unread%s" % id
        #log.msg("Looking up unread messages count for session user %s from viewdb" % key)
        return self.viewdb.get(key).addCallback(self.extract_unread, request, key)
    
    def hash_unread(self, request, id):
        id = self._read_session(request)
        return 'unread_%s' % id
                
    def extract_unread(self, result, request, id):
        log.msg("Extracting unread count from %s" % repr(result))
        if len(result) > 1:
            value = result and result[1] or "0"
        else:
            value = result or "0"
        output = {'count' : value}
        key = self.hash_unread(request, id)
        self.cache.set({key: output}, 60) # one minute
        #log.msg("Set twice cache key %s as %s" % ('count', value))
        return output
        
    def valid_unread(self, request, id, value):
        return True

    # geo location
    def hash_geo(self, request, id):
        return 'ip'
        
    # ip address
    def hash_ip(self, request, id):
        return 'ip'

    # User Session    
    
    def hash_session(self, request, id):
        id = self._read_session(request)
        if id:
            return 'session_' + id
        else:
            return ''
      
    def fetch_session(self, request, id):
        id = self._read_session(request)
        return self.db.runInteraction(self._session, id).addCallback(self.extract_session, request, id)
    
    def extract_session(self, result, request, id):
        if len(result) and len(result[0]):
            session = dict(zip(result[0][0].keys(), result[0][0].values()))
            session.update(dict(result[1]))
            output = session
        else:
            output = {}
        key = self.hash_session(request, id)
        self.cache.set({key : output}, 86400) # 24 hours
        return output
    
    def valid_session(self, request, id, value):
        return True
    
    def _read_session(self, request):
        return urllib.unquote(request.getCookie(self.config['session_cookie']) or '')

    def _session(self, txn, id):
        #log.msg('Looking up session %s' % id)
        users_query = "select * from users where id = %s" % id
        log.msg('Running query: %s' % users_query)
        txn.execute(users_query)
        users_result = txn.fetchall()
        return [users_result]
        
    # AB testing calls
    def loadAbTestingGroups(self):
        query = "select test_name, values_list from ab_testing_groups;"
        self.db.runQuery(query).addCallback(self.addAbTestingGroups)
        reactor.callLater(60.0, self.loadAbTestingGroups)

    def addAbTestingGroups(self, rows):
        self.abTestingGroups = {}
        for row in rows:
            try:
                test_name, values_list = row[0], row[1]
                values = [value.split(":") for value in values_list.split(",")]
                self.abTestingGroups[test_name] = values
            except:
                log.msg("Error parsing ab test row %s" % repr(row))
        log.msg("Finished adding ab testing groups from %s" % repr(rows))

    def hash_abdependency(self, request, ignored=None):
        return "abdependency_%s" % request.uri.rstrip("?")
    
    def fetch_abdependency(self, request, ignored=None):
        return None
    
    def valid_abdependency(self, request, id, value):
        return True
        
    # ab tests are hashed by the current session and the test_name
    def hash_abvalue(self, request, ignored=None):
        return "abvalue_%s" % self.read_ab_cookie(request)

    def fetch_abvalue(self, request, ignored=None):
        log.msg('Looking up ab group: %s' % self.hash_abvalue(request))
        if not hasattr(self, 'viewdb'):
            log.msg("Not connected to the viewdb")
            return self.extract_abvalue(None, request)
        # we permanently save your ab group to the memcachedb box so that it's never lost
        return self.viewdb.get(self.hash_abvalue(request)).addCallback(self.extract_abvalue, request)

    def valid_abvalue(self, request, id, value):
        return True

    def extract_abvalue(self, result, request):
        log.msg("Extracting abvalue from %s" % repr(result))
        if result and result[1]:
            try:
                output = pickle.loads(result[1])
            except:
                log.msg("Exceptin depickling result")
                traceback.print_exc()
                output = {}
        else:
            output = {}
        updated = False # whether we've assigned any new ab groups and need to resave to memcache
        for test_name, value_list in self.abTestingGroups.iteritems():
            if test_name not in output:
                output[test_name] = self.pick_ab(value_list)
                updated = True # picked a new ab value, need to resave
        key = self.hash_abvalue(request)
        self.cache.set({key: output}, 300) # 5 minutes
        if updated and hasattr(self, 'viewdb'):
            self.viewdb.set(key, pickle.dumps(output)) # 30 days
        log.msg("EXTRACTED ABVALUE %s" % repr(output))
        return output

    def read_ab_cookie(self, request):
        ab_id = request.getCookie(self.config['ab_cookie'])
        if not ab_id:
            ab_id = self.gen_ab_id()
            log.msg("Generating new ab cookie: %s" % ab_id)
            request.addCookie(self.config['ab_cookie'], ab_id)
            request.addCookie(self.config['new_ab_cookie'], 'True')
        return ab_id

    def new_ab_cookie(self, request):
        return request.getCookie(self.config['new_ab_cookie']) == 'True'

    def gen_ab_id(self):
        return ''.join(random.sample('abcdefghijklmnopqrstuvwxyz123456790', 25))

    # choose a random item by weights for a test
    def pick_ab(self, value_list):
        n = sum(float(weight) for value, weight in value_list) * random.uniform(0, 1)        
        test_value = ""
        for value, weight in value_list:
            test_value = value
            if n < float(weight):
                return test_value
            n = n - float(weight)
        return test_value

