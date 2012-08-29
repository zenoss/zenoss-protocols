##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################

# Deletes an AMQP queue using the credentials stored in
# $ZENHOME/etc/global.conf.

import sys
from contextlib import closing
from optparse import OptionParser
from amqplib.client_0_8.connection import Connection


def usage():
    return "Usage: %prog <queue_name> [...]"


def main():
    parser = OptionParser(usage=usage())
    options, args = parser.parse_args()

    if not args:
        print "No queues specified"
        parser.print_usage()
        sys.exit(1)

    # Nested import/functions to prevent unnecessary overhead when no
    # queues are specified
    import Globals
    from Products.ZenUtils.GlobalConfig import getGlobalConfiguration
    global_conf = getGlobalConfiguration()
    hostname = global_conf.get('amqphost', 'localhost')
    port     = global_conf.get('amqpport', '5672')
    username = global_conf.get('amqpuser', 'zenoss')
    password = global_conf.get('amqppassword', 'zenoss')
    vhost    = global_conf.get('amqpvhost', '/zenoss')
    ssl      = global_conf.get('amqpusessl', '0')
    use_ssl  = True if ssl in ('1', 'True', 'true') else False

    cxn = Connection(host="%s:%s" % (hostname, port),
                      userid=username,
                      password=password,
                      virtual_host=vhost,
                      ssl=use_ssl)

    with closing(cxn) as conn, closing(conn.channel()) as channel:
        for queue in args:
            print "Removing queue: %s" % queue
            try:
                channel.queue_delete(queue)
            except Exception as e:
                print "ERROR: Unable to remove %s; does it exist?" % queue
                sys.exit(1)


if __name__ == "__main__":
    main()
