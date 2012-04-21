
import stats_buffer
import util

class ConnectionTrend:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            trend = []
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                trend.append((node, a, vals[-1]))
            result[bucket] = trend
        return result

class CalcTrend:
    def run(self, accessor):
        result = {}
        for bucket, stats_info in stats_buffer.buckets.iteritems():
            values = stats_info[accessor["scale"]][accessor["counter"]]
            timestamps = values["timestamp"]
            timestamps = [x - timestamps[0] for x in timestamps]
            nodeStats = values["nodeStats"]
            samplesCount = values["samplesCount"]
            trend = []
            for node, vals in nodeStats.iteritems():
                a, b = util.linreg(timestamps, vals)
                trend.append((node, a))
            result[bucket] = trend
        return result

NodeCapsule = [
   {"name" : "Number of Connection",
    "ingredients" : [
        {
            "description" : "Connection Trend",
            "counter" : "curr_connections",
            "type" : "python",
            "scale" : "minute",
            "code" : "ConnectionTrend",
            "threshold" : {
                "high" : 10000,
            },
        },
     ]
    },
    {"name" : "OOM Error",
     "ingredients" : [
        {
            "description" : "OOM Errors",
            "counter" : "ep_oom_errors",
            "type" : "python",
            "scale" : "hour",
            "code" : "CalcTrend",
        },
        {
            "description" : "Temporary OOM Errors",
            "counter" : "ep_tmp_oom_errors",
            "type" : "python",
            "scale" : "hour",
            "code" : "CalcTrend",
        },
     ]
    },
     {"name" : "Overhead",
     "ingredients" : [
        {
            "description" : "Overhead",
            "counter" : "ep_overhead",
            "type" : "python",
            "scale" : "hour",
            "code" : "CalcTrend",
        },
     ]
    },   
    
]


