import dbaccessor
import stats_buffer
import util
counter_name = 'disk_write_queue'

class AvgDiskQueue:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            #print bucket, stats_info
            disk_queue_avg_error = []
            dsik_queue_avg_warn = []
            values = stats_info[accessor["scale"]][accessor["counter"]]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                avg = sum(vals) / samplesCount
                if avg > accessor["threshold"]["high"]:
                    disk_queue_avg_error.append({"node":node, "level":"red", "value":avg})
                elif avg > accessor["threshold"]["low"]:
                    dsik_queue_avg_warn.append({"node":node, "level":"yellow", "value":avg})
            if len(disk_queue_avg_error) > 0:
                result[bucket] = {"error" : disk_queue_avg_error}
            if len(dsik_queue_avg_warn) > 0:
                result[bucket] = {"warn" : disk_queue_avg_warn}
        return result

class DiskQueueTrend:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            trend_error = []
            trend_warn = []
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                if a > accessor["threshold"]["high"]:
                    trend_error.append({"node":node, "level":"red", "value":a})
                elif a > accessor["threshold"]["low"]:
                    trend_warn.append({"node":node, "level":"yellow", "value":a})
            if len(trend_error) > 0:
                result[bucket] = {"error" : trend_error}
            if len(trend_warn) > 0:
                result[bucket] = {"warn" : trend_warn}
        return result

class TapQueueTrend:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            trend_error = []
            trend_warn = []
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                if a > accessor["threshold"]["high"]:
                    trend_error.append({"node":node, "level":"red", "value":a})
                elif a > accessor["threshold"]["low"]:
                    trend_warn.append({"node":node, "level":"yellow", "value":a})
            if len(trend_error) > 0:
                result[bucket] = {"error" : trend_error}
            if len(trend_warn) > 0:
                result[bucket] = {"warn" : trend_warn}
        return result

DiskQueueCapsule = [
    {"name" : "DiskQueueDiagnosis",
     "description" : "",
     "ingredients" : [
        {
            "name" : "avgDiskQueueLength",
            "description" : "Persistence severely behind - averge disk queue length is above threshold",
            "counter" : "disk_write_queue",
            "pernode" : True,
            "scale" : "minute",
            "type" : "python",
            "code" : "AvgDiskQueue",
            "threshold" : {
                "low" : 50000000,
                "high" : 1000000000
            },
        },     
        {
            "name" : "diskQueueTrend",
            "description" : "Persistence severely behind - disk write queue continues growing",
            "counter" : "disk_write_queue",
            "pernode" : True,
            "scale" : "hour",
            "type" : "python",
            "code" : "DiskQueueTrend",
            "threshold" : {
                "low" : 0,
                "high" : 0.25
            },
        },
     ],
     "indicator" : True,
    },
    {"name" : "ReplicationTrend",
     "ingredients" : [
        {
            "name" : "replicationTrend",
            "description" : "Replication severely behind - ",
            "counter" : "ep_tap_total_total_backlog_size",
            "pernode" : True,
            "scale" : "hour",
            "type" : "python",
            "code" : "TapQueueTrend",
            "threshold" : {
                "low" : 0,
                "high" : 0.2
            },
        }
     ],
     "indicator" : True,
    },
]