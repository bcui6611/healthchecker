#!/usr/bin/env python
# -*- coding: utf-8 -*-

import getopt
import sys
import os
import traceback
import copy

import dbaccessor
import analyzer
import stats_buffer
import util

import listservers
import buckets
import node
import info
import util_cli as util
import mc_bin_client
import simplejson

import node_map

def parse_opt():
    (cluster, user, password) = ('', '','')

    try:
        (opts, _args) = getopt.getopt(sys.argv[1:], 
                                      'c:dp:u:', [
                'cluster=',
                'debug',
                'password=',
                'user='
                ])
    except getopt.GetoptError, err:
        usage(err)

    for (opt, arg) in opts:
        if opt in ('-c', '--cluster'):
            cluster = arg
        if opt in ('-u', '--user'):
            user = arg
        if opt in ('-p', '--password'):
            password = arg
        if opt in ('-d', '--debug'):
            debug = True
    if not cluster: 
        usage("please provide a CLUSTER, or use -h for more help.")
    return (cluster, user, password, opts)

def get_stats(mc, stats):
    try:
        node_stats = mc.stats('')
        if node_stats:
            for key, val in node_stats.items():
                stats[key] = val
    except Exception, err:
        #print "ERROR: command: %s: %s:%d, %s" % ('stats all', server, port, err)
        traceback.print_exc()
        #sys.exit(1)
    
    try:
        node_stats = mc.stats('tap')
        if node_stats:
            for key, val in node_stats.items():
                stats[key] = val
    except Exception, err:
        #print "ERROR: command: %s: %s:%d, %s" % ('stats tap', server, port, err)
        traceback.print_exc()
        #sys.exit(1)

def stats_formatter(stats, prefix=" ", cmp=None):
    if stats:
        longest = max((len(x) + 2) for x in stats.keys())
        for stat, val in sorted(stats.items(), cmp=cmp):
            s = stat + ":"
            print "%s%s%s" % (prefix, s.ljust(longest), val)

def collect_data():

    (cluster, user, password, opts) = parse_opt()
    server, port = util.hostport(cluster)

    nodes = []
    commands = {
        'host-list'         : listservers.ListServers,
        'server-info'       : info.Info,
        'bucket-list'       : buckets.Buckets,
        'bucket-stats'      : buckets.BucketStats,
        'bucket-node-stats' : buckets.BucketNodeStats,
        }
    
    accessor = dbaccessor.DbAccesor()

    accessor.connect_db()
    accessor.create_databases();

    #get node list and its status
    try:
        cmd = 'host-list'
        c = commands[cmd]()
        nodes = c.runCmd(cmd, server, port, user, password, opts)
    except Exception, err:
        print "ERROR: command: %s: %s:%d, %s" % (cmd, server, port, err)
        sys.exit(1)

    #get each node information
    try:
        cmd = 'server-info'
        c = commands[cmd]()
        for node in nodes:
            (node_server, node_port) = util.hostport(node['hostname'])
            if node_map.address_map.has_key(node_server):
                node_server = node_map.address_map[node_server]
            nodeid = accessor.create_or_update_node(node_server, node_port, node['status'], server)
            if node['status'] == 'healthy':
                node_info = c.runCmd(cmd, node_server, node_port, user, password, opts)
                accessor.process_node_stats(nodeid, node_info)
                #stats = {}
                #mc = mc_bin_client.MemcachedClient(node_server, node['ports']['direct'])
                #get_stats(mc, stats)
            else:
                print "Unhealthy node: %s:%s" %(node_server, node['status'])
    except Exception, err:
        traceback.print_exc()
        #print "ERROR: command: %s: %s:%d, %s" % (cmd, server, port, err)
        sys.exit(1)

    #get each bucket information
    try:
        cmd = 'bucket-list'
        c = commands[cmd]()
        json = c.runCmd(cmd, server, port, user, password, opts)
        for bucket in json:
            (bucket_name, bucket_id) = accessor.process_bucket(bucket, server)

            # get bucket related stats
            cmd = 'bucket-stats'
            c = buckets.BucketStats(bucket_name)
            json = c.runCmd(cmd, server, port, user, password, opts)
            stats_buffer.buckets_summary[bucket_name] = json

            #retrieve bucket stats per node
            stats_buffer.buckets[bucket_name] = copy.deepcopy(stats_buffer.stats)
            cmd = 'bucket-node-stats'
            for scale, stat_set in stats_buffer.buckets[bucket_name].iteritems():
                for stat in stat_set.iterkeys():
                    print "retieving: ", stat, " scale:", scale
                    c = buckets.BucketNodeStats(bucket_name, stat, scale)
                    json = c.runCmd(cmd, server, port, user, password, opts)
                    stats_buffer.buckets[bucket_name][scale][stat] = json
                #accessor.process_bucket_node_stats(bucket_id, server, stat, json)
    except Exception, err:
        traceback.print_exc()
        #print "ERROR: command: %s: %s:%d, %s" % (cmd, server, port, err)
        sys.exit(1)
    
    accessor.close()

def main():
    
    #make snapshot for the current cluster status
    collect_data()

    #analyze the snapshot and historic data
    performer = analyzer.StatsAnalyzer()
    performer.run_analysis()
    performer.run_report()

if __name__ == '__main__':
    main()
