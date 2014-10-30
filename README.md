This tool is used to provide a dynamic DNS for docker container.

It's purpose is to run on a docker hosting server, and expect a https://www.powerdns.com to run.

It's written and python and uses https://github.com/docker/docker-py.

Each time a container is started, it updates is database, and powerdns must be configured to use it.

One should add in /etc/pdns/pnds.conf:

    launch=...,remote
    recursor=8.8.8.9                 # a forwarder
    allow-recursion=169.254.0.0/16   # allow the containers to query
    remote-connection-string=unix:path=/var/run/dockerdns.socket
    

It's configuration is given in a dockerdns.ini

    [main]
    domain = local    # domain for container
    reverse = 169.254 #<containers IP prefix>
    pidfile = /var/run/dockerdns.pid
    
    [powerdns]
    user = pnds
    group = pnds
    socket = /var/run/dockerdns.socket
    
    [docker]
    socket = /var/run/docker.sock
    api = 1.13
 
It run as a daemon with a simple :

    .../dockerdns.py -c /etc/dockerdns.ini