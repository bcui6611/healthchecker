import dbaccessor
import stats_buffer
import util
counter_name = 'disk_write_queue'

class AvgDiskQueue:
    def run(self, accessor):
        disk_queue_avg = []

        for bucket, stats_info in stats_buffer.buckets.iteritems():
            #print bucket, stats_info
            values = stats_info[accessor["scale"]][accessor["counter"]]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            for node, vals in nodeStats.iteritems():
                avg = sum(vals) / samplesCount
                disk_queue_avg.append((bucket, node, avg))
        return disk_queue_avg

    def action(self, values, thresholds):
        flags = []
        for bucket, node, avg in values:
            if avg < thresholds["low"]:
                flags.append((bucket, node, "green"))
            elif avg >= thresholds["low"] and avg < thresholds["high"]:
                flags.append((bucket, node, "yellow"))
            else:
                flags.append((bucket, node, "red"))
        return flags

class DiskQueueTrend:
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

class TapQueueTrend:
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

DiskQueueCapsule = [
    {"name" : "Disk Queue Diagnosis",
     "ingredients" : [
        {
            "description" : "Avg Disk queue length",
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
            "description" : "Disk queue trend",
            "counter" : "disk_write_queue",
            "pernode" : True,
            "scale" : "hour",
            "type" : "python",
            "code" : "DiskQueueTrend",
        },
     ]
    },
    {"name" : "Replication trend",
     "ingredients" : [
        {
            "description" : "Replication Trend",
            "counter" : "ep_tap_total_total_backlog_size",
            "pernode" : True,
            "scale" : "hour",
            "type" : "python",
            "code" : "TapQueueTrend",
        }
     ]
    },
]