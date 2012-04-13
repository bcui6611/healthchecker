import dbaccessor
import stats_buffer
import util

class DGMRatio:
    def run(self, accessor):
        hdd = accessor.execute("SELECT sum(usedbyData) FROM StorageInfo WHERE type='hdd'")
        ram = accessor.execute("SELECT sum(usedbyData) FROM StorageInfo WHERE type='ram'")
        if ram[0] > 0:
            ratio = hdd[0] / ram[0]
        else:
            ratio = 0
        return ratio

class ARRatio:
    def run(self, accessor):
        active = accessor.execute("SELECT sum(currentItems) FROM SystemStats")
        replica = accessor.execute("SELECT sum(replicaCurrentItems) FROM SystemStats")
        if replica[0] > 0:
            ratio = active[0] / replica[0]
        else:
            ratio = 0
        return ratio

class OpsRatio:
    def run(self, accessor):        
        ops_avg = {
            "cmd_get": [],
            "cmd_set": [],
            "delete_hits" : [],
        }
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            for counter in accessor["counter"]:
                values = stats_info[accessor["scale"]][counter]
                nodeStats = values["nodeStats"]
                samplesCount = values["samplesCount"]
                for node, vals in nodeStats.iteritems():
                    avg = sum(vals) / samplesCount
                    ops_avg[counter].append((bucket, node, avg))
        result = []
        for read, write, delete in zip(ops_avg['cmd_get'], ops_avg['cmd_set'], ops_avg['delete_hits']):

            total = read[2] + write[2] + delete[2]
            if total == 0:
                result.append((read[0], read[1], "0:0:0"))
            else:
                read_ratio = read[2] *100 / total 
                write_ratio = write[2] * 100 / total
                del_ratio = delete[2] * 100 /total
                result.append((read[0], read[1], "{0}:{1}:{2}".format(read_ratio, write_ratio, del_ratio)))

        return result

class CacheMissRatio:
    def run(self, accessor):
        trend = []
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                trend.append((bucket, node, a, b))
        return trend

class ItemGrowth:
    def run(self, accessor):
        trend = []
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                avg = sum(vals) / samplesCount
                trend.append((bucket, node, a, b, avg))
        return trend

class NumVbuckt:
    def run(self, accessor):
        trend = []
        for bucket, stats_info in stats_buffer.buckets_summary.iteritems():
            total, values = stats_buffer.retrieveSummaryStats(bucket, accessor["counter"])
            trend.append((bucket, values[-1]))
        return trend

ClusterCapsule = [
    {"name" : "Node Status",
     "ingredients" : [
        {
            "description" : "Number of Nodes",
            "type" : "SQL",
            "code" : "SELECT count(*) FROM ServerNode"
        },
        {
            "description" : "Number of Down Nodes",
            "type" : "SQL",
            "code" : "SELECT count(*) FROM ServerNode WHERE status='down'"
        },
        {
            "description" : "Number of Warmup Nodes",
            "type" : "SQL",
            "code" : "SELECT count(*) FROM ServerNode WHERE status='warmup'"
        }
      ]
    },
    {"name" : "Total Data Size",
     "ingredients" : [
        {
            "description" : "Total Data Size",
            "type" : "SQL",
            "code" : "SELECT sum(usedbyData) FROM StorageInfo WHERE type='hdd'"
        }
     ]
    },
    {"name" : "DGM",
     "ingredients" : [
        {
            "description" : "DGM Ratio",
            "type" : "SQL",
            "code" : "DGMRatio"
        },
     ]
    },
    {"name" : "Active / Replica Resident Ratio",
     "ingredients" : [
        {
            "description" : " A/R Ratio",
            "type" : "python",
            "code" : "ARRatio"
        },
     ]
    },
    {"name" : "OPS performance",
     "ingredients" : [
        {
            "description" : "Read/Write/Delete ops ratio",
            "type" : "python",
            "scale" : "minute",
            "counter" : ["cmd_get", "cmd_set", "delete_hits"],
            "code" : "OpsRatio"
        },
     ]
    },
    {"name" : "Cache Miss Ratio",
     "ingredients" : [
        {
            "description" : "Cache miss ratio",
            "counter" : "ep_cache_miss_rate",
            "type" : "python",
            "scale" : "hour",
            "code" : "CacheMissRatio"
        },
     ]
    },
    {"name" : "Growth Rate",
     "ingredients" : [
        {
            "description" : "Growth rate for items",
            "counter" : "curr_items",
            "type" : "python",
            "scale" : "day",
            "code" : "ItemGrowth"
        },
     ]
    },
    {"name" : "VBucket number",
     "ingredients" : [
        {
            "description" : "Active VBucket number",
            "counter" : "vb_active_num",
            "type" : "python",
            "scale" : "summary",
            "code" : "NumVbuckt"
        },
        {
            "description" : "Replica VBucket number",
            "counter" : "vb_replica_num",
            "type" : "python",
            "scale" : "summary",
            "code" : "NumVbuckt"
        },
     ]
    },
]


