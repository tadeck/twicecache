from __future__ import with_statement
from twisted.internet import reactor, defer, protocol
from twisted.python import log
import sys, urllib, time, re, traceback, os, time
import cPickle as pickle
import parser, storage, http, cache, mail

try:
    import GeoIP
    gi = GeoIP.new(GeoIP.GEOIP_MEMORY_CACHE)
except:
    mail.error('Unable to load GeoIP library:\n%s' % traceback.format_exc())

# for adding commas to strings
comma_re = re.compile(r"(?:\d*\.)?\d{1,3}-?")

request_actions = ['page', 'session', 'geo', 'ip']
key_fetch_actions = ['memcache', 'viewdb']
hash_fetch_actions = ['abvalue']
session_actions = ['session', 'favorite', 'subscription', 'unread']


# acts as a fake dictionary for <& get geo ip &>
class GeoLookup:
    def __init__(self, request, connection):
        self.request = request
        self.connection = connection
        self.geos = dict()
        
    def get(self, ip="ip"):
        if ip not in self.geos: 
            try:
                if ip == "ip":
                    lookup = self.request.getRemoteIp(self.connection)
                else:
                    lookup = ip
                self.geos[ip] = gi.country_code_by_addr(lookup)
            except:
                pass
        return self.geos[ip]

# acts as a fake dictionary for <& get ip current &>
class IpLookup:
    def __init__(self, request, connection):
        self.request = request
        self.connection = connection
        self.ip = None
        
    def get(self, ignored=None):
        if not self.ip:
            self.ip = self.request.getRemoteIp(self.connection)
        return self.ip
        
class MessageSender(protocol.Protocol):
    def __init__(self, message):
        self.message = message

    def connectionMade(self):
        # log.msg("Sending message to logging server: %s" % self.message)
        # send the message
        self.transport.write(self.message)
        # close the connection
        self.transport.loseConnection()

class RequestHandler(http.HTTPRequestDispatcher):
    def __init__(self, config):
        # Caches and config
        self.config = config
        
        # Template format
        self.specialization_re = re.compile(self.config['template_regex'])
        
        # Language redirects
        self.allowed_languages = ['en', 'ko', 'hi', 'ma', 'ca', 'de', 'es', 'fr', 'it', 'nl', 'pt', 'pt-br', 'sk', 'tl', 'vi', 'ar', 'ru', 'zh-cn', 'zh-tw']
        
        # uniques counting
        self.uniques = {}
        if 'uniques_file' in self.config:
            if os.path.exists(self.config['uniques_file']):
                with open(config['uniques_file']) as f:
                    self.uniques = pickle.load(f)
            self.pruneUniques()
        
        # Data Store
        log.msg('Initializing data store...')
        self.store = storage.DataStore(config)
    
    # once per minute remove old uniques
    def pruneUniques(self):
        try:
            yesterday = time.time() - 24 * 60 * 60
            for ip, timestamp in self.uniques.items():
                if timestamp < yesterday:
                    del self.uniques[ip]
            with open(self.config['uniques_file'], "w") as f:
                pickle.dump(self.uniques, f, pickle.HIGHEST_PROTOCOL)
        except:
            log.msg('ERROR: Unable to prune uniques!')
            traceback.print_exc()
        reactor.callLater(60.0, self.pruneUniques)
    
    def objectReceived(self, connection, request):
        "Main request handler"            
        # Handle mark dirty requests
        if request.getHeader(self.config.get('purge_header')) is not None:
            self.markDirty(connection, request)
            return
        
        if hasattr(self, 'uniques'):
            # record this unique request
            self.uniques[request.getRemoteIp(connection)] = time.time()
                    
        # Handle request for the current uniques count
        if 'live/uniques_list' in request.uri:
            connection.sendCode(200, ','.join(self.uniques.keys()))
            return
        
        # Handle time requests
        if 'live/time' in request.uri:
            connection.sendCode(200, str(time.time()))
            return
        # Handle language redirects
        lang = request.getHeader('accept-language')
        if lang:
            lang = lang.replace(' ', '').split(';')[0].split(',')[0].lower()
        host = request.getHeader('x-real-host')
        if host and host.split('.')[0] == self.config.get('default_host', 'www') and \
          lang and lang.lower() in self.allowed_languages and \
          lang.lower() != 'en' and \
          request.method.upper() not in self.store.uncacheable_methods:
            response = http.HTTPObject()
            response.status = 302
            response.setHeader('Location', 'http://%s.mydomain.com%s' % (lang, request.uri))
            connection.transport.write(response.writeResponse())
            connection.shutdown()
            log.msg('REDIRECT: lang %s host %s -> http://%s.mydomain.com%s' % (lang, host, lang, request.uri))

        # Check cache
        else:
            # Overwrite host field
            real_host = self.config.get('rewrite_host', request.getHeader('x-real-host'))
            if real_host:
                request.setHeader('host', real_host)
            try:
                mygeo = GeoLookup(request, connection).get()
                request.setHeader('x-geo', mygeo)
            except:
                log.msg("Failed to set geo header")
                
            # Add in prefetch keys
            keys = []
            keys.append(self.store.elementHash(request, 'expiration'))
            keys.append(self.store.elementHash(request, 'abvalue'))
            keys.append(self.store.elementHash(request, 'abdependency'))
            session_key = self.store.elementHash(request, 'session')
            if session_key:
                keys.append(session_key)

            # Retrieve keys
            log.msg('PREFETCH: %s' % keys)
            self.store.get(keys, request).addCallback(self.getPage, connection, request)
            
# ---------- CACHE EXPIRATION -----------
            
    def markDirty(self, connection, request):
        global session_actions
        "Mark a uri as dirty"
        uri = request.uri
        try:
            kind = request.getHeader(self.config.get('purge_header')).lower()
        except:
            log.msg('Could not read expiration type: %s' % repr(request.getHeader(self.config.get('purge_header'))))
            return
        log.msg("Expire type: %s, arg: %s" % (kind, uri))
        # Parse request
        if kind == '*':
            self.store.flush()
            log.msg('Cleared entire cache')
        elif kind == 'url':
            try:
                key = self.store.elementHash(request, 'expiration')
                self.store.cache.set({key : time.time()}, 86400)
                log.msg('Expired all variants of %s' % uri)
            except:
                log.msg('Could not delete variants of %s' % uri)
        elif kind == 'session':
            try:
                types = session_actions
                keys = ['%s_%s' % (t, uri[1:]) for t in types]
                self.store.delete(keys)
                log.msg('Deleted session-related keys: %s' % keys)
            except:
                pass
        else:
            try:
                key = kind + '_' + uri[1:]
                self.store.delete(key)
                log.msg('Deleted %s_%s' % (kind, uri[1:]))
            except:
                pass
        # Write response
        connection.sendCode(200, "Expired %s_%s" % (kind, uri))
        return True       
                
# ---------- CLIENT RESPONSE -----------  

    def find_prefix(self, elements, prefix, include_key=False):
        matches = [(key, val) if include_key else val for key, val in elements.iteritems() if key.startswith(prefix)]
        return matches and matches[0]


    def getPage(self, elements, connection, request):
        # Set the abvalue we get on the request
        abvalue = self.find_prefix(elements, 'abvalue_')
        abvalue_string = ','.join('%s:%s' % (k, v) for k, v in abvalue.iteritems())
        log.msg("Saving abvalue header %s: %s" % (self.config.get('abvalue_header'), repr(abvalue_string)))
        request.setHeader(self.config.get('abvalue_header'), abvalue_string)

        # hash the page to the appropriate abgroup
        abdependency = self.find_prefix(elements, 'abdependency_') or []
        page_key = self.store.hash_page(request, abvalue = abvalue, abdependency = abdependency)

        self.store.get(page_key, request).addCallback(self.checkPage, connection, request, elements)
        

    def checkPage(self, elements, connection, request, extra = {}):
        "See if we have the correct version of the page"       
        elements.update(extra)         

        rkey, rval = self.find_prefix(elements, 'page_', include_key=True)
        if 'response' not in rval:
            connection.sendCode(408, 'Request timed out.')
            return True
        response = rval['response']
        cookies = sorted((response.getHeader(self.config.get('cookies_header')) or '').split(','))

        # Extract the abdependency and abvalue if we have one
        abdependency = self.find_prefix(elements, 'abdependency_') or []
        abvalue = self.find_prefix(elements, 'abvalue_')
        
        key = self.store.hash_page(request, cookies = cookies, abdependency = abdependency, abvalue = abvalue)

        # If the page we fetched doesn't have the right cookies or ab_values, try again!
        if key != self.store.hash_page(request, abdependency = abdependency, abvalue = abvalue):
            del elements[rkey]
            return self.store.get(key, request).addCallback(self.scanPage, connection, request, elements)
                    
        # If the page is expired, request a new copy
        expire_time = self.find_prefix(elements, 'expiration_')
        if expire_time and rval['rendered_on'] < expire_time:
            log.msg('EXPIRED: rendered_on %s, expire_time %s' % (rval['rendered_on'], expire_time))
            del elements[rkey]
            return self.store.get(key, request, force=True).addCallback(self.checkPage, connection, request, elements)
        
        return self.scanPage(elements, connection, request)

    def scanPage(self, elements, connection, request, extra = {}):
        global request_actions, key_fetch_actions, hash_fetch_actions
        "Scan for missing elements"
        elements.update(extra)
        logged_in = self.find_prefix(elements, 'session_') is not None
        data = self.find_prefix(elements, 'page_')['response'].body
        matches = self.specialization_re.findall(data)
        missing_keys = []
        for match in matches:
            # Parse element
            try:
                parts = match.strip().split()
                command = parts[0].lower()
                element_type = parts[1].lower()
                element_id = parts[2]
            except:
                mail.error('Error in scanPage:\n%s' % traceback.format_exc())
                continue
            log.msg("Matched element (%s %s %s)" % (command, element_type, element_id))
            if element_type not in request_actions:
                if element_type in key_fetch_actions or element_type in hash_fetch_actions or logged_in:
                    key = self.store.elementHash(request, element_type, element_id)
                    if key and key not in missing_keys:
                        missing_keys.append(key)
        if missing_keys:
            log.msg("Fetching missing keys %s" % repr(missing_keys))
            d = self.store.get(missing_keys, request)
            d.addCallback(self.renderPage, connection, request, elements)
        else:
            self.renderPage({}, connection, request, elements)

    def renderPage(self, new_elements, connection, request, elements):
        global session_actions, key_fetch_actions, hash_fetch_actions
        "Write the page out to the request's connection"
        elements.update(new_elements)
        # unread must come after session
        for etype in ['page'] + session_actions + hash_fetch_actions:
            eitems = [val for key, val in elements.items() if key.startswith(etype)]
            if eitems:
                eitems = eitems[0]
            else:
                eitems = {}
            setattr(self, 'current_' + etype, eitems)
            #log.msg('Current %s: %s' % (etype, eitems))

        for etype in key_fetch_actions:
            log.msg('elements: %s' % elements.items())
            eitems = dict((self.store.elementId(key), val) for key, val in elements.iteritems() if key.startswith(etype))
            setattr(self, 'current_' + etype, eitems)
            log.msg('Current %s: %s' % (etype, eitems))
        
        self.current_geo = GeoLookup(request, connection)
        self.current_ip  = IpLookup(request, connection)
        
        response = self.current_page['response']
        # Do Templating
        data = self.specialization_re.sub(self.specialize, response.body)
        # Remove current stuff
        for etype in session_actions:
            setattr(self, 'current_' + etype, {})
        # Log
        app_server = response.getHeader('x-app-server') or 'unknown'
        log.msg('RENDER %s [%s] (%.3fs from %s)' % (response.status, request.uri, (time.time() - request.received_on), app_server.strip()))
        # Overwrite headers
        response.setHeader('connection', 'close')
        response.setHeader('content-length', len(data))
        response.setHeader('via', 'Twice %s %s:%s' % (self.config['version'], self.config['hostname'], self.config['port']))
        # Send geo along to user
        try:
            response.setHeader('x-geo', self.current_geo.get())
        except:
            pass
        # Send the new ab cookie along, if we have one
        if self.store.new_ab_cookie(request):
            response.addCookie(self.config['ab_cookie'], self.store.read_ab_cookie(request))
        # Delete twice/cache headers
        response.removeHeader(self.config.get('cache_header'))
        response.removeHeader(self.config.get('twice_header'))
        response.removeHeader(self.config.get('cookies_header'))
        response.removeHeader('x-app-server')
        # Write response
        connection.transport.write(response.writeResponse(body = data))
        connection.shutdown()
        


# ---------- TEMPLATING -----------

    def specialize(self, expression):
        "Parse an expression and return the result"
        try:
            expression = expression.groups()[0].strip()
            parts = expression.split()
            # Syntax is: command target arg1 arg2 argn
            #   command - one of 'get', 'if', 'unless', 'incr', 'decr'
            #   target - one of 'memcache', 'session'
            #   arg[n] - usually the name of a key
            command, target, args = parts[0].lower(), parts[1], parts[2:]
            #log.msg('command: %s target: %s args: %s' % (command, target, repr(args)))
        except:
            mail.error('Could not parse expression: [%s]' % expression)
            return expression
        # Grab dictionary
        try:
            dictionary = getattr(self, 'current_' + target)
        except:
            dictionary = {}
        
        actual_args = []
        filters = []
        
        next_filter = False
        for arg in args:
            if arg == "|":
                next_filter = True
            elif next_filter:
                filters.append(arg)
            else:
                actual_args.append(arg)
        
        args = actual_args
        return_val = None
        
        #log.msg('dictionary: %s' % dictionary)
        # Handle commands
        if command == 'get' and len(args) >= 1:
            if len(args) >= 2:
                default = args[1]
            else:
                default = ''
            val = dictionary.get(args[0])
            if not val:
                val = default
            #log.msg('arg: %s val: %s (default %s)' % (args[0], val, default))
            return_val = str(val)
        elif command == 'pop' and len(args) >= 1:
            if len(args) >= 2:
                default = args[1]
            else:
                default = ''
            val = dictionary.get(args[0])
            if not val:
                val = default
            else:
                try:
                    getattr(self.store, command + '_delete')(args[0])
                except:
                    mail.error('Data store is missing %s_delete' % command)
            return_val = str(val)
        elif command == 'if' and len(args) >= 2:
            if dictionary.get(args[0]):
                return_val = str(args[1])
            elif len(args) >= 3:
                return_val = str(args[2])
            else:
                return_val = ''
        elif command == 'unless' and len(args) >= 2:
            if not dictionary.get(args[0]):
                return_val = str(args[1])
            elif len(args) >= 3:
                return_val = str(args[2])
            else:
                return_val = ''
        elif (command == 'incr' or command == 'decr') and len(args) >= 1:
            try:
                func = getattr(self.store, command + '_' + target)
                set_func = getattr(self.store, 'set_' + target)
            except:
                mail.error('Data store is missing %s_%s or set_%s' % (command, target, target))
                return_val = ''
            val = dictionary.get(args[0])
            if val:
                try:
                    func(args[0])
                    if command == 'incr':
                        dictionary[args[0]] = int(val) + 1
                    else:
                        dictionary[args[0]] = int(val) - 1
                except:
                    pass
            elif len(args) >= 2:
                set_func(args[0], args[1])
                dictionary[args[0]] = args[1]
            return_val = ''
        else:
            log.msg('Invalid command: %s' % command)
            return_val = expression
        
        return self.apply_filters(return_val, filters)
    
    def apply_filters(self, value, filters):
        for filter in filters:
            if filter == "js": value = value.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')
            elif filter == "html": value = value.replace("<", "&lt;").replace(">", "&gt;").replace('&', '&amp;')
            elif filter == "comma": value = ','.join(comma_re.findall(value[::-1]))[::-1]
            else: pass
        return value
                    
