##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


from __future__ import absolute_import
import logging
from zenoss.protocols.interfaces import IAMQPConnectionInfo
from zope.interface import implements

log = logging.getLogger('zen.protocols')

class AMQPConfig(object):

    implements(IAMQPConnectionInfo)

    """
    Class that encapsulates the configuration for
    all of the various queues that zenoss uses.
    """
    _options = [
        dict(short_opt='H', long_opt='amqphost', type='string', key='host', default='localhost', help='Rabbitmq server host'),
        dict(short_opt='P', long_opt='amqpport', type='int', key='port', default=5672, help='Rabbitmq server port', parser=int),
        dict(short_opt='V', long_opt='amqpvhost', type='string', key='vhost', default='/zenoss', help='Rabbitmq server virtual host'),
        dict(short_opt='u', long_opt='amqpuser', type='string', key='user', default='zenoss', help='User to connect as'),
        dict(short_opt='p', long_opt='amqppassword', type='string', key='password', default='zenoss', help='Password to connect with'),
        dict(short_opt='s', long_opt='amqpusessl', action='store_true', key='usessl', default=False, help='Use SSL to connect to the server', parser=lambda v: str(v).lower() in ('true', 'y', 'yes', '1')),
        dict(short_opt='b', long_opt='amqpheartbeat', type='int', key='amqpconnectionheartbeat', default=300, help='AMQP Connection Heart Beat in Seconds', parser=int),
    ]

    def __init__(self, amqphost='localhost', amqpport=5672, amqpvhost='/zenoss', amqpuser='zenoss',
                 amqppassword='zenoss', amqpusessl=False, amqpconnectionheartbeat=300):
        """
        Initialize with optional settings as keyword arguments.
        
        See AMQPConfig._options for list of valid options.
        """
        self._host = amqphost
        self._port = amqpport
        self._vhost = amqpvhost
        self._user = amqpuser
        self._password = amqppassword
        self._usessl = amqpusessl
        self._optionMap = None
        self._amqpconnectionheartbeat =  amqpconnectionheartbeat

    def _getOptionMap(self):
        """
        Create a map of options to their storage keys. Used for fast lookup of options.
        """
        if self._optionMap is None:
            self._optionMap = {}
            for option in self._options:
                self._optionMap[option['long_opt']] = option

        return self._optionMap

    def update(self, options):
        """
        Update the settings from a dictionary.

        See AMQPConfig._options for list of valid options.    
        """
        optionMap = self._getOptionMap()

        if isinstance(options, dict):
            options = options.iteritems()
        elif isinstance(options, object):
            # Looks like a plain old object with properties as options
            options = options.__dict__.iteritems()

        for key, value in options:
            if key in optionMap:
                parser = optionMap[key].get('parser', str)
                setattr(self, '_' + optionMap[key]['key'], parser(value))

    @property
    def host(self):
        return self._host

    @property
    def amqpconnectionheartbeat(self):
        return self._amqpconnectionheartbeat

    @property
    def port(self):
        return self._port

    @property
    def vhost(self):
        return self._vhost

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password

    @property
    def usessl(self):
        return self._usessl
        
    @classmethod
    def addOptionsToParser(cls, parser):
        """
        Populate an OptionPaser with our options.
        """
        for option in cls._options:
            parser.add_option(
                '-' + option['short_opt'],
                '--' + option['long_opt'],
                type=option.get('type'),
                dest=option['long_opt'],
                help=option['help'],
                default=option['default'],
                action=option.get('action', 'store'),
            )
        return parser
