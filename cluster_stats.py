import dbaccessor
import stats_buffer
import util

class ExecSQL:
    def run(self, accessor, stmt):
        result = accessor.execute(stmt)
        return result[0]

class NodeList:
    def run(self, accessor):
        return accessor.execute("SELECT host, port, version, os, status FROM ServerNode", True)

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
        result = {}
        cluster = 0
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            item_avg = {
                "curr_items": [],
                "vb_replica_curr_items": [],
            }
            for counter in accessor["counter"]:
                values = stats_info[accessor["scale"]][counter]
                nodeStats = values["nodeStats"]
                samplesCount = values["samplesCount"]
                for node, vals in nodeStats.iteritems():
                    avg = sum(vals) / samplesCount
                    item_avg[counter].append((node, avg))
            res = []
            active_total = replica_total = 0
            for active, replica in zip(item_avg['curr_items'], item_avg['vb_replica_curr_items']):
                if replica[1] == 0:
                    res.append((active[0], "No replica"))
                else:
                    ratio = 1.0 * active[1] / replica[1] 
                    res.append((active[0], util.pretty_float(ratio)))
                active_total += active[1]
                replica_total += replica[1]
            if replica_total == 0:
                res.append(("total", "no replica"))
            else:
                ratio = active_total * 1.0 / replica_total
                cluster += ratio
                res.append(("total", util.pretty_float(ratio)))
            result[bucket] = res
        result["cluster"] = util.pretty_float(cluster / len(stats_buffer.buckets))
        return result

class OpsRatio:
    def run(self, accessor):        
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            ops_avg = {
                "cmd_get": [],
                "cmd_set": [],
                "delete_hits" : [],
            }
            for counter in accessor["counter"]:
                values = stats_info[accessor["scale"]][counter]
                nodeStats = values["nodeStats"]
                samplesCount = values["samplesCount"]
                for node, vals in nodeStats.iteritems():
                    avg = sum(vals) / samplesCount
                    ops_avg[counter].append((node, avg))
            res = []
            read_total = write_total = del_total = 0
            for read, write, delete in zip(ops_avg['cmd_get'], ops_avg['cmd_set'], ops_avg['delete_hits']):
                count = read[1] + write[1] + delete[1]
                if count == 0:
                    res.append((read[0], "0:0:0"))
                else:
                    read_ratio = read[1] *100 / count
                    read_total += read_ratio
                    write_ratio = write[1] * 100 / count
                    write_total += write_ratio
                    del_ratio = delete[1] * 100 / count
                    del_total += del_ratio
                    res.append((read[0], "{0}:{1}:{2}".format(read_ratio, write_ratio, del_ratio)))
            read_total /= len(ops_avg['cmd_get'])
            write_total /= len(ops_avg['cmd_set'])
            del_total /= len(ops_avg['delete_hits'])
            res.append(("total", "{0}:{1}:{2}".format(read_total, write_total, del_total)))
            result[bucket] = res

        return result

class CacheMissRatio:
    def run(self, accessor):
        result = {}
        cluster = 0
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            trend = []
            total = 0
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                value = a * timestamps[-1] + b
                total += value
                trend.append((node, util.pretty_float(value)))
            total /= len(nodeStats)
            trend.append(("total", util.pretty_float(total)))
            cluster += total
            result[bucket] = trend
        result["cluster"] = util.pretty_float(cluster / len(stats_buffer.buckets))
        return result

class ItemGrowth:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            trend = []
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                if b < 1:
                   trend.append((node, 0))
                else:
                    start_val = b
                    end_val = a * timestamps[-1] + b
                    rate = (end_val * 1.0 / b - 1.0) * 100
                    trend.append((node, util.pretty_float(rate)))
            result[bucket] = trend
        return result

class AvgItemSize:
    def run(self, accessor):
        return 0

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
            "description" : "Node list",
            "type" : "pythonSQL",
            "code" : "NodeList",
        },
        {
            "description" : "Number of Nodes",
            "type" : "SQL",
            "stmt" : "SELECT count(*) FROM ServerNode",
            "code" : "ExecSQL",
        },
        {
            "description" : "Number of Down Nodes",
            "type" : "SQL",
            "stmt" : "SELECT count(*) FROM ServerNode WHERE status='down'",
            "code" : "ExecSQL",
        },
        {
            "description" : "Number of Warmup Nodes",
            "type" : "SQL",
            "stmt" : "SELECT count(*) FROM ServerNode WHERE status='warmup'",
            "code" : "ExecSQL",
        },
        {
            "description" : "Number of Nodes failed over",
            "type" : "SQL",
            "stmt" : "SELECT count(*) FROM ServerNode WHERE clusterMembership != 'active'",
            "code" : "ExecSQL",
        },
      ],
      "clusterwise" : True,
      "perNode" : False,
      "perBucket" : False,
    },
    {"name" : "Total Data Size",
     "ingredients" : [
        {
            "description" : "Total Data Size across cluster",
            "type" : "SQL",
            "stmt" : "SELECT sum(usedbyData) FROM StorageInfo WHERE type='hdd'",
            "code" : "ExecSQL",
            "unit" : "GB",
        }
     ],
     "clusterwise" : True,
     "perNode" : False,
     "perBucket" : False,
    },
    {"name" : "Available disk space",
     "ingredients" : [
        {
            "description" : "Available disk space",
            "type" : "SQL",
            "stmt" : "SELECT sum(free) FROM StorageInfo WHERE type='hdd'",
            "code" : "ExecSQL",
            "unit" : "GB",
        }
     ],
     "clusterwise" : True,
     "perNode" : False,
     "perBucket" : False,
    },
   {"name" : "Cache Miss Ratio",
     "ingredients" : [
        {
            "description" : "Cache miss ratio",
            "counter" : "ep_cache_miss_rate",
            "type" : "python",
            "scale" : "hour",
            "code" : "CacheMissRatio",
            "unit" : "percentage",
        },
     ],
     "clusterwise" : True,
     "perNode" : True,
     "perBucket" : True,
    },
    {"name" : "DGM",
     "ingredients" : [
        {
            "description" : "Disk to Memory Ratio",
            "type" : "pythonSQL",
            "code" : "DGMRatio"
        },
     ],
     "clusterwise" : True,
     "perNode" : False,
     "perBucket" : False,
    },
    {"name" : "Active / Replica Resident Ratio",
     "ingredients" : [
        {
            "description" : "Active and Replica Residentcy Ratio",
            "type" : "python",
            "counter" : ["curr_items", "vb_replica_curr_items"],
            "scale" : "minute",
            "code" : "ARRatio",
        },
     ],
     "clusterwise" : True,
     "perNode" : True,
     "perBucket" : True,
    },
    {"name" : "OPS performance",
     "ingredients" : [
        {
            "description" : "Read/Write/Delete ops ratio",
            "type" : "python",
            "scale" : "minute",
            "counter" : ["cmd_get", "cmd_set", "delete_hits"],
            "code" : "OpsRatio",
        },
     ]
    },
    {"name" : "Growth Rate",
     "ingredients" : [
        {
            "description" : "Data Growth rate for items",
            "counter" : "curr_items",
            "type" : "python",
            "scale" : "day",
            "code" : "ItemGrowth",
            "unit" : "percentage",
        },
     ]
    },
    {"name" : "Average Document Size",
     "ingredients" : [
        {
            "description" : "Average Document Size",
            "type" : "python",
            "code" : "AvgItemSize",
            "unit" : "KB",
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


