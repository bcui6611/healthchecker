#!/usr/bin/env python
# -*- coding: utf-8 -*-

from usage import usage

import restclient

rest_cmds = {
    'bucket-list': '/pools/default/buckets',
    'bucket-stats': '/pools/default/buckets/{0}/stats?zoom=hour',
    'bucket-node-stats': '/pools/default/buckets/{0}/stats/{1}?zoom={2}'
    }
methods = {
    'bucket-list': 'GET',
    'bucket-stats': 'GET',
    'bucket-node-stats': 'GET',
    }

class Buckets:
    def __init__(self):
        self.debug = False
        self.rest_cmd = rest_cmds['bucket-list']
        self.method = 'GET'

    def runCmd(self, cmd, server, port,
               user, password, opts):
        self.user = user
        self.password = password

        bucketname = ''
        buckettype = ''
        authtype = 'sasl'
        bucketport = '11211'
        bucketpassword = ''
        bucketramsize = ''
        bucketreplication = '1'
        output = 'default'

        for (o, a) in opts:
            if o == '-b' or o == '--bucket':
                bucketname = a
            if o == '--bucket-type':
                buckettype = a
            if o == '--bucket-port':
                bucketport = a
            if o == '--bucket-password':
                bucketpassword = a
            if o == '--bucket-ramsize':
                bucketramsize = a
            if o == '--bucket-replica':
                bucketreplication = a
            if o == '-d' or o == '--debug':
                self.debug = True
            if o in  ('-o', '--output'):
                output = a
        self.rest_cmd = rest_cmds[cmd]
        rest = restclient.RestClient(server, port, {'debug':self.debug})

        # get the parameters straight

        opts = {}
        opts['error_msg'] = "unable to %s" % cmd
        opts['success_msg'] = "%s" % cmd
        data = rest.restCmd(methods[cmd], self.rest_cmd,
                            self.user, self.password, opts)

        return rest.getJson(data)

class BucketStats:
    def __init__(self, bucket_name):
        self.debug = False
        self.rest_cmd = rest_cmds['bucket-stats'].format(bucket_name)
        self.method = 'GET'

    def runCmd(self, cmd, server, port,
               user, password, opts):
        opts = {}
        opts['error_msg'] = "unable to %s" % cmd
        opts['success_msg'] = "%s" % cmd

        #print server, port, cmd, self.rest_cmd
        rest = restclient.RestClient(server, port, {'debug':self.debug})
        data = rest.restCmd(methods[cmd], self.rest_cmd,
                            user, password, opts)
        return rest.getJson(data)

class BucketNodeStats:
    def __init__(self, bucket_name, stat_name, scale):
        self.debug = False
        self.rest_cmd = rest_cmds['bucket-node-stats'].format(bucket_name, stat_name, scale)
        self.method = 'GET'
        #print self.rest_cmd

    def runCmd(self, cmd, server, port,
               user, password, opts):
        opts = {}
        opts['error_msg'] = "unable to %s" % cmd
        opts['success_msg'] = "%s" % cmd

        #print server, port, cmd, self.rest_cmd
        rest = restclient.RestClient(server, port, {'debug':self.debug})
        data = rest.restCmd(methods[cmd], self.rest_cmd,
                            user, password, opts)
        return rest.getJson(data)

