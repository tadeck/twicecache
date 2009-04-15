from twisted.python import usage, log
import sys, traceback, mail

class Options(usage.Options):
    optFlags = [
        ['verbose', 'v', 'Verbose mode'],
        ['daemon', 'd', 'Daemonize'],
    ]
    optParameters = [
        ['config', 'c', 'twice.conf', 'Config file'],
        ['log', 'l', '', 'Log file'],
        ['port', 'p', '', 'Port to listen on'],
        ['backend_webserver', 'w', '', 'Backend webserver to request from'],
        ['interface', 'i', '', 'Interface to bind to']
    ]

def parsefile(fname, settings):
    try:
        # Read config file
        basename = '/'.join(fname.split('/')[:-1])
        if basename:
            basename += '/'
        else:
            basename = ''
        data = file(fname).readlines()
        for line in data:
            args = line.split()
            if len(args) >= 2 and not line.startswith('#'):
                if args[0] == 'include':
                    [parsefile(basename+fname.strip(), settings) for fname in args[1].split(',')]
                    continue
                elif args[0] not in settings or args[0][0] == '!':
                    if args[0][0] == '!': args[0] = args[0][1:]
                    val = args[1].strip()
                else:
                    val = settings[args[0]] + ',' + args[1].strip()
                if val.lower() in ['yes', 'true']: val = True
                elif val.lower() in ['no', 'false']: val = False
                settings[args[0]] = val
    except:
        log.msg('Unable to parse config file %s' % fname)
        log.err()
        sys.stdout.flush()
        raise

def parse():
    "Parse settings into a dict"
    try:
        options = Options()
        options.parseOptions()
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.stdout.flush()
        sys.exit(1)
    try:
        # Read config file
        settings = {}
        parsefile(options['config'], settings)
        # Override with command line args
        for option, val in options.items():
            if option not in settings or val: settings[option] = val
        return settings
    except:
        sys.exit(1)

