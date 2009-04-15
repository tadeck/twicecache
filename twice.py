from twisted.python import log
from twisted.manhole import telnet
import sys, os, signal, traceback, resource, socket, mail

__author__    = "Kyle Vogt <kyle@justin.tv> and Emmett Shear <emmett@justin.tv>"
__version__   = "0.2"
__copyright__ = "Copyright (c) 2008, Justin.tv, Inc."
__license__   = "MIT"        

def check_memory(limit):
    try:
        cpu, mem = [i.replace('\n', '') for i in os.popen('ps -p %s -o pcpu,rss' % os.getpid()).readlines()[1].split(' ') if i]
        real = int(mem) / 1000.0
        if real > limit:
            log.msg('Using too much memory (%.2fMB out of %.2fMB)' % (real, limit))
            os.kill(os.getpid(), signal.SIGTERM)
    except:
        log.msg('Unable to read memory or cpu usage!')
        traceback.print_exc()
    reactor.callLater(15.0, check_memory, limit)
    
class Logger:

    def __init__(self, filename):
        self.log_filename = filename
        self.setup_log(filename)
        try:
            signal.signal(signal.SIGUSR1, self.signal_handler) 
        except:
            log.msg('Error assigning signal handler:\n%s' % traceback.format_exc())

    def setup_log(self, name):       
        try: 
            self.log_file = open(name, 'a')
            self.log_observer = log.FileLogObserver(self.log_file)
            log.startLoggingWithObserver(self.log_observer.emit)
        except:
            msg = "Error in setup_log:\n%s" % traceback.format_exc()
            print msg
            mail.error(msg)

    def signal_handler(self, signo, frame): 
        try:
            log.msg('Rotating log %s' % self.log_filename)
            log.removeObserver(self.log_observer.emit)
            self.log_file.close()
            self.setup_log(self.log_filename)   
        except:
            msg = "Error in signal_handler:\n%s" % traceback.format_exc()
            print msg
            mail.error(msg)     
        
if __name__ == '__main__':

    # Read config
    import parser
    config = parser.parse()
    config['hostname'] = socket.gethostname()
    config['version'] = __version__
    
    # Log
    log_file = config.get('log', 'stdout')
    if log_file != 'stdout':
        logger = Logger(log_file)
    else:
        log.startLogging(sys.stdout)

    # Set up reactor
    try:
        from twisted.internet import epollreactor
        epollreactor.install()
        log.msg('Using epoll')
    except:
        log.msg('Cannot use epoll!')
        traceback.print_exc()
    from twisted.internet import reactor  
    
    # Step up to maximum file descriptor limit
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        for i in xrange(16):
            test = 2 ** i
            if test > hard: break
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (test, hard))        
                val = test
            except ValueError:
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                val = soft
                break
        log.msg('%s file descriptors available (system max is %s)' % (val, hard))
    except:
        log.msg('Error setting fd limit!')
        traceback.print_exc()
        
    # Check memory usage
    check_memory(int(config.get('memory_limit', 1000)))
        
    # Start request handler event loop
    try:
        import handler
        factory = handler.RequestHandler(config)
        reactor.listenTCP(int(config['port']), factory)
    except:
        mail.error('Error starting handler!\n%s' % traceback.format_exc())
    
    shell = telnet.ShellFactory()
    shell.username = 'twice'
    shell.password = 'twice'
    try:
        reactor.listenTCP(4040, shell)
        log.msg('Telnet server running on port 4040.')
    except:
        log.msg('Telnet server not running.')
            
    # Run
    reactor.run()        

