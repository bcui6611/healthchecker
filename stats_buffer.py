
buckets_summary = {}
stats_summary = {
    'vb_active_num' : {},
    'vb_replica_num' : {},
    'curr_connections' : {},
}

buckets = {}
stats = {
    "minute" : {
        'disk_write_queue' : {},
        'cmd_get' : {},
        'cmd_set' : {},
        'delete_hits' : {},
        #'curr_items_tot' : {},
        #'curr_connections' : {},
    }, 
    "hour" : {
        'disk_write_queue' : {},
        'ep_cache_miss_rate' : {},
        'ep_tap_total_total_backlog_size' : { },
    }, 
    "day" : {
        'curr_items' : {},
    },
}

def retrieveSummaryStats(bucket, cmd):
    samples = buckets_summary[bucket]["op"]["samples"]
    for sample_key in samples.keys():
        if cmd == sample_key:
            total_samples = buckets_summary[bucket]["op"]["samplesCount"]
            return (total_samples, samples[cmd])
    print "Unknown stats:", cmd
    return (0, [])
