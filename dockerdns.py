#!/usr/bin/python

import pytz

import docker as dockerlib
import json
import datetime

import socket
import select
import threading
import platform

import optparse
from pwd import getpwnam

import atexit
import os
import resource
import signal
import sys
import logging
import time
from logging.handlers import SysLogHandler
from requests.exceptions import ConnectionError

import ConfigParser


class Singleton(object):
    """
    A non-thread-safe helper class to ease implementing singletons.
    This should be used as a decorator -- not a metaclass -- to the
    class that should be a singleton.

    The decorated class can define one `__init__` function that
    takes only the `self` argument. Other than that, there are
    no restrictions that apply to the decorated class.

    To get the singleton instance, use the `Instance` method. Trying
    to use `__call__` will result in a `TypeError` being raised.

    Limitations: The decorated class cannot be inherited from.

    """

    def __init__(self, decorated):
        self._decorated = decorated

    def Instance(self):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.

        """
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)


def sig_handler(signum, frame):
    """
    This method will be called by signal handler, mark the process as needs to stop
    """
    Watchdog.Instance().relaunch = False


@Singleton
class Watchdog(object):
    """
    A watchdog class that fork a subprocess and monitor it, restart it if dies from a core.
    It can also manage some cleanup
    """

    def __init__(self):
        self.files_to_clean = set()
        self.pid_to_stop = -1
        self.relaunch = True
        self._childuid = False
        self._rundir = "/"
        self.inchild = False
        self._pidfile = None

    @property
    def pidfile(self):
        """The file where the watcher process pid is stored."""
        return self._pidfile

    @pidfile.setter
    def pidfile(self, value):
        self._pidfile = value
        self.files_to_clean.add(value)

    @pidfile.deleter
    def pidfile(self):
        del self._pidfile

    def add_file(self, file):
        """
        Add a file to unlink at the end
        """
        self.files_to_clean.add(file)

    def set_child(self, pid):
        self.pid_to_stop = pid

    @property
    def rundir(self):
        return self._rundir

    @rundir.setter
    def rundir(self, value):
        self._rundir = value

    @property
    def childuid(self):
        return self._childuid

    @childuid.setter
    def childuid(self, value):
        self._childuid = value

    def _finish(self):
        # If in the worker process, do nothing
        if self.inchild:
            return

        logging.debug('doing bookeeping')

        # Don't kill at random, and don't kill 1 (init)
        if self.pid_to_stop > 2:
            os.kill(self.pid_to_stop, signal.SIGTERM)

        for filename in self.files_to_clean:
            try:
                os.unlink(filename)
            except IOError:
                logging.error("Unable to unlink %s", filename)

    def reset(self):
        self.files_to_clean = set()
        self.pid_to_stop = -1
        self.inchild = True

    def create_daemon(self):
        """
        Transform the running process in a daemon, doing the double fork and all the needed magic.
        When the method return the current process as lost stdin, stdout, is a new session leader.
        It's not the starting process anymore
        """
        # do the UNIX double-fork magic, see Stevens' "Advanced
        # Programming in the UNIX Environment" for details (ISBN 0201563177)
        # http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        try:
            # Fork a child process so the parent can exit.
            pid = os.fork()
        except OSError, e:
            raise Exception, "%s [%d]" % (e.strerror, e.errno)

        if pid == 0:  # The first child.
            os.setsid()

            try:
                pid = os.fork()  # Fork a second child.
            except OSError, e:
                raise Exception, "%s [%d]" % (e.strerror, e.errno)

            if pid == 0:  # The second child.
                os.chdir(self.rundir)
                os.umask(0)
            else:
                os._exit(0)
        else:
            os._exit(0)

        # All the termination are done using os._exit, to ignore the exit handlers

        # A daemon is never localized
        os.environ.setdefault("LC_ALL", "POSIX")

        # Default maximum for the number of available file descriptors.
        MAXFD = 1024

        # Close all open file descriptors.
        maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
        if maxfd == resource.RLIM_INFINITY:
            maxfd = MAXFD

        # Iterate through and close all file descriptors.
        for fd in range(0, maxfd):
            try:
                os.close(fd)
            except OSError:  # ERROR, fd wasn't open to begin with (ignored)
                pass

        # Redirect the standard I/O file descriptors to the specified file.  Since
        # the daemon has no controlling terminal, most daemons redirect stdin,
        # stdout, and stderr to /dev/null.  This is done to prevent side-effects
        # from reads and writes to the standard I/O file descriptors.

        # The standard I/O file descriptors are redirected to /dev/null by default.
        if (hasattr(os, "devnull")):
            REDIRECT_TO = os.devnull
            # This call to open is guaranteed to return the lowest file descriptor,
            # which will be 0 (stdin), since it was closed above.
            os.open(REDIRECT_TO, os.O_RDWR)  # standard input (0)
            # Duplicate standard input to standard output and standard error.
            os.dup2(0, sys.stdout.fileno())  # standard output (1)
            os.dup2(0, sys.stderr.fileno())  # standard error (2)

        logging.info('process forked')

        #Create the pid file if defined
        if self.pidfile is not None:
            pid = os.getpid()
            try:
                file(self.pidfile, 'w+').write("%s\n" % pid)
            except Exception, e:
                logging.error("Access monitor daemon failed to write pid file: %s" % e)
                sys.exit(1)

        return 0

    def run_child(self):
        atexit.register(Watchdog.Instance()._finish)
        oldsignal = signal.signal(signal.SIGTERM, sig_handler)

        self.relaunch = True
        while self.relaunch:
            # Launch the watchdog
            try:
                pid_worker = os.fork()  # Fork a second child.
            except OSError, e:
                raise Exception, "%s [%d]" % (e.strerror, e.errno)

            if (pid_worker == 0):
                # Code running in the worker process
                if self.childuid:
                    os.seteuid(self.childuid)
                # Reset the termination handler, nothing is done in the child
                self.reset()
                signal.signal(signal.SIGTERM, oldsignal)
                logging.info("starting a worker child")
                # Return from the method, the caller process flow continue in the caller
                return
            else:
                # Code running in the monitoring process
                self.set_child(pid_worker)
                try:
                    # Wait for worker process to run
                    (pid, status) = os.waitpid(-1, 0)
                    child_signal = status & 0x7F
                    child_exit = status >> 8
                    child_core = (status & 0x80) != 0
                    logging.warn("got a dead child: %d with %d/%d", pid, child_exit, child_signal)
                    # Child exited, no need to kill it
                    self.set_child(-1)
                    # if normal exit, don't restart
                    if status == 0 or status > 255 or status & 255 == 9:
                        self.relaunch = False
                    else:
                        time.sleep(1)
                except OSError:
                    # got an interruption
                    pass
                except:
                    logging.error("Unexpected error: %s", sys.exc_info())
                    self.relaunch = False
        # the relaunch loop ended, termination
        logging.info('stopped')
        sys.exit()


def log_to_syslog(name, facility=SysLogHandler.LOG_DAEMON):
    logger = logging.getLogger()

    # Manually define the syslog socket
    # Needed because SysLogHandler use network socket, that's not a good idea
    if os.path.exists('/dev/log'):
        # The common unix case
        syslogaddress = '/dev/log'
    elif os.path.exists('/var/run/syslog'):
        # The mac os case
        syslogaddress = '/var/run/syslog'
    else:
        # Don't know where to look, try network
        syslogaddress = ('localhost', 514)
    handler = logging.handlers.SysLogHandler(address=syslogaddress, facility=facility)

    format_string = '%s[%%(process)d]: %%(message)s' % name
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)

    logger.addHandler(handler)

def ip_to_reverse(ip):
        reversed = ip.split(".")
        reversed.reverse()
        reversed = "%s.in-addr.arpa" % ".".join(reversed)
        return reversed

class DNS(object):
    infos = {}

    def __init__(self, domain, reverse_domain):
        self.domain = domain
        self.reverse_domain = reverse_domain

    def add_rr(self, name, rr, info):
        if not name in self.infos:
            self.infos[name] = {}
        self.infos[name][rr] = info

    def add_host(self, hostname, ip):
        self.add_rr("%s.%s" % (hostname, self.domain), "A", ip)
        reversed = ip_to_reverse(ip)
        self.add_rr(reversed, "PTR", "%s.%s" % (hostname, self.domain))

    def remove_host(self, hostname, ip):
        fqdn = "%s.%s" % (hostname, self.domain)
        reversed = ip_to_reverse(ip)
        self.infos.pop(fqdn, None)
        self.infos.pop(reversed, None)

    def get_rr(self, query, type):
        if not query in self.infos:
            raise StopIteration
        if type == "ANY":
            for (key, value) in self.infos[query].items():
                yield key, value
        elif type in self.infos[query]:
            yield type, self.infos[query][type]


PowerDns_functions = {}


def powerdns_method(func, *args, **kwargs):
    PowerDns_functions[func.__name__] = func
    return func


class PowerDns(object):
    def __init__(self, dns):
        self.dns = dns
        self.hostname = platform.node()
        self.fqdn = "%s.%s" % (self.hostname, self.dns.domain)

    def execute(self, arg_string):
        query = json.loads(arg_string)
        if query['method'] in PowerDns_functions:
            method = PowerDns_functions[query['method']]
        else:
            method = PowerDns.fail
        value = method(self, query['parameters'])
        if value is None:
            return '{"result": false}'
        else:
            return json.dumps({'result': value})

    @powerdns_method
    def initialize(self, parameters):
        return True

    @powerdns_method
    def lookup(self, parameters):
        qname = parameters['qname']
        if not qname.endswith(self.dns.domain) and not qname.endswith(self.dns.reverse_domain):
            return False
        qtype = parameters['qtype']
        if qtype == 'SOA' and ( qname == self.dns.domain or qname == self.dns.reverse_domain):
            return [{"qtype": "SOA", "qname": qname, "content": self.dosoa(), "ttl": 600}]
        elif qname == "1.0.254.169.in-addr.arpa":
            return [{"qtype": "PTR", "qname": qname, "content": "%s" % self.fqdn, "ttl": 600}]
        else:
            values = []
            for (atype, qanswer) in self.dns.get_rr(qname, qtype):
                values.append({"qtype": atype, "qname": qname, "content": qanswer, "ttl": 10})
            return values

    @powerdns_method
    def list(self, parameters):
        axfr = []
        axfr.append({"qtype": "SOA", "name": self.dns.domain, "content": self.dosoa()})
        axfr.append({"qtype": "SOA", "name": self.dns.reverse_domain, "content": self.dosoa()})
        for name, rrs in self.dns.infos.items():
            for qtype, content in rrs.items():
                axfr.append({"qtype": qtype, "qname": name, "content": content, "ttl": 10})
        return axfr

    def fail(self, parameters):
        return False

    def dosoa(self):
        serial = 1
        refresh = 43200
        retry = 5
        expire = 43200
        negative = 5
        content = "%s. hostmaster.%s. %d %d %d %d %d" % (
            self.fqdn, self.dns.domain, serial, refresh, retry, expire, negative)
        return content


class ConnectionListener(threading.Thread):
    def __init__(self, cnx, addr, server, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.daemon = True
        self.cnx = cnx
        self.addr = addr
        self.server = server

    def run(self):
        while True:
            datagram = self.cnx.recv(4096).strip()
            logging.debug("<- '%s'" % datagram)
            # Nothing read; powerdns is gone
            if len(datagram) <= 0:
                break
            value = self.server.execute(datagram)
            self.cnx.send(value)
            logging.debug("-> '%s'" % value)


class PowerDnsListener(threading.Thread):
    def __init__(self, server, path="/var/run/dockerdns.socket", pdnsuid=498, pdnsgid=497, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.daemon = True
        if os.path.exists(path):
            os.remove(path)
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(path)
        self.socket.listen(10)
        os.chown(path, pdnsuid, pdnsgid)
        self.server = server

    def run(self):
        while True:
            conn, addr = self.socket.accept()
            listener = ConnectionListener(conn, addr, self.server)
            listener.start()


class Event(object):
    def __init__(self, **kwargs):
        self.time = datetime.datetime.fromtimestamp(kwargs['time'], pytz.utc)
        del kwargs['time']
        for attrname in kwargs.keys():
            setattr(self, attrname, kwargs[attrname])
        self.Id = self.id

    def __str__(self):
        return "%s %s" % (self.time.strftime("%c"), self.status)


class AutoObject(object):
    def __init__(self, **kwargs):
        for attr_name in kwargs.keys():
            setattr(self, attr_name, kwargs[attr_name])

    def check_attr(self, name, new_object, kwargs):
        value = kwargs.pop(name, None)
        if value is not None:
            instance = new_object(**value)
            setattr(self, name, instance)
        else:
            setattr(self, name, None)


class ContainerConfig(AutoObject):
    def __init__(self, **kwargs):
        AutoObject.__init__(self, **kwargs)


class NetworkSetting(AutoObject):
    def __init__(self, **kwargs):
        AutoObject.__init__(self, **kwargs)


class Container(AutoObject):
    @staticmethod
    def construct(cnx, id):
        container_serialized = cnx.inspect_container(id)
        return Container(**container_serialized)

    def __init__(self, **kwargs):
        self.check_attr('NetworkSettings', NetworkSetting, kwargs)
        self.check_attr('Config', ContainerConfig, kwargs)
        AutoObject.__init__(self, **kwargs)
        self.id = self.Id

    def __str__(self):
        return "%s %s %s" % (self.id, self.State, self.Config.Cmd[0])


Docker_functions = {}


def docker_status(func, *args, **kwargs):
    Docker_functions[func.__name__] = func
    return func


class Docker(object):
    containers = {}

    def __init__(self, dns, base_url='unix://var/run/docker.sock', version='1.13', timeout=10):
        self.c = dockerlib.Client(base_url=base_url,
                                  version=version,
                                  timeout=timeout)
        self.dns = dns
        for i in self.c.containers(all=True):
            container = Container.construct(self.c, i["Id"])
            self.containers[container.id] = container

            if container.NetworkSettings is not None:
                ip = container.NetworkSettings.IPAddress
                hostname = container.Config.Hostname
                if len(ip) > 0:
                    dns.add_host(hostname, ip)

    def loop(self):
        for e in self.c.events():
            buffer = json.loads(e)
            event = Event(**buffer)
            value = self.do(event)
            if value is not None:
                yield (event, value)

    def do(self, event):
        if event.status in Docker_functions:
            return Docker_functions[event.status](self, event)

    @docker_status
    def create(self, event):
        container_json = self.c.inspect_container(event.id)
        cont = Container(**container_json)
        self.containers[event.id] = cont
        return cont

    @docker_status
    def start(self, event):
        container = self.create(event)
        ip = container.NetworkSettings.IPAddress
        hostname = container.Config.Hostname
        self.dns.add_host(hostname, ip)
        return container

    @docker_status
    def die(self, event):
        if event.id in self.containers:
            old_container = self.containers[event.id]
            ip = old_container.NetworkSettings.IPAddress
            hostname = old_container.Config.Hostname
            self.dns.remove_host(hostname, ip)
        container = self.create(event)
        return container

    @docker_status
    def destroy(self, event):
        return self.containers.pop(event.id, None)

def main():
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config", dest="config", help="Config file path", default=None, action="store")
    parser.add_option("-f", "--foreground", dest="daemonize", help="Stay in foreground, logging to stdout",
                      default=True, action="store_false")
    parser.add_option("-d", "--debug", dest="debug", help="set loglevel to debug", default=None, action="store_true")
    parser.add_option("-s", "--docker_socket", dest="docker_socket", help="socker for communication with docker",
                      default=None, action="store")
    parser.add_option("-D", "--domain", dest="domain", help="domain for servers", default=None, action="store")
    parser.add_option("-U", "--powerdns_user", dest="powerdns_user", help="User for power dns", default=None,
                      action="store")
    parser.add_option("-G", "--powerdns_group", dest="powerdns_group", help="Group for power dns", default=None,
                      action="store")
    parser.add_option("-S", "--powerdns_socket", dest="powerdns_socket", help="Powerdns socket path",
                      default=None, action="store")

    (options, args) = parser.parse_args()

    # prepare config object and default values
    config = type('', (), {})()
    config.domain = 'local'
    config.reverse = '169.254'

    config.pidfile = '/var/run/dockerdns/dockerdns.pid'
    config.docker_socket = '/var/run/docker.sock'
    config.docker_api = '1.13'
    config.powerdns_socket = '/var/run/dockerdns/dockerdns.socket'
    config.powerdns_user = 'pdns'
    config.powerdns_group = 'pdns'

    params = {
        'powerdns': ('user', 'group', 'socket'),
        'docker': ('socket', 'api'),
        'main': ('domain', 'reverse', 'pidfile'),
    }

    if options.config is not None:
        config_file = ConfigParser.SafeConfigParser()
        config_file.read(options.config)
        for (section, options_keys) in params.items():
            if config_file.has_section(section):
                if section == main:
                    section_prefix = ''
                else:
                    section_prefix = "%s_" % section
                for option_key in options_keys:
                    if config_file.has_option(section, option_key):
                        setattr(config, "%s%s" % (section_prefix, option_key), config_file.get(section, option_key))
        #if config_file.has_section('powerdns'):
        #    for option in ('user', 'group', 'socket'):
        #        if config_file.has_option('powerdns', option):
        #            setattr(config, "powerdns_%s" % option, config_file.get('powerdns', option))
        #if config_file.has_section('docker'):
        #    for option in ('socket', 'api'):
        #        if config_file.has_option('docker', option):
        #            setattr(config, "docker_%s" % option, config_file.get('docker', option))
        #if config_file.has_section('main'):
        #    for option in ('domain', 'reverse'):
        #        if config_file.has_option('main', option):
        #            setattr(config, "key", config_file.get('main', option))

    # Merge the config with values from command line
    for (key, value) in vars(options).items():
        config_attrs = vars(config).keys()
        if value is not None and key in config_attrs:
            setattr(config, key, value)

    config.reverse_domain = ip_to_reverse(config.reverse)

    if options.debug:
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logging.debug("logging debug messages")
    else:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

    try:
        pdns_uid = getpwnam(config.powerdns_user).pw_uid
    except KeyError:
        logging.error("user '%s' not found" % config.powerdns_user)
        exit(1)

    try:
        pdns_gid = getpwnam(config.powerdns_user).pw_gid
    except KeyError:
        logging.error("group '%s' not found" % config.powerdns_user)
        exit(1)

    if options.daemonize:
        watcher = Watchdog.Instance()
        watcher.pidfile = config.pidfile
        watcher.create_daemon()
        log_to_syslog('dockerdns')
        watcher.add_file(config.powerdns_socket)
        watcher.run_child()

    dns = DNS(config.domain, config.reverse_domain)
    power_dns = PowerDns(dns=dns)
    server = PowerDnsListener(server=power_dns, pdnsuid=pdns_uid, pdnsgid=pdns_gid, path=config.powerdns_socket)
    server.start()

    # define a sleep time for communication failure
    failure_pause = 1
    while True:
        try:
            docker = Docker(dns=dns, base_url='unix:/%s' % config.docker_socket, version=config.docker_api)

            for (event, container) in docker.loop():
                logging.debug("%s: %s " % (event, container))
            # reset sleep time
            failure_pause = 1
        except ValueError:
            # socket closed, see https://github.com/docker/docker-py/pull/385
            pass
        except ConnectionError as e:
            logging.error("communication failure with docker daemon: %s" % e)
            time.sleep(failure_pause)
            failure_pause *= 2
        except Exception as e:
            logging.error("unexpected error with docker: %s" % e)

# no global name space pollution
try:
    if __name__ == '__main__':
        sys.exit(main())
except KeyboardInterrupt:
    pass
