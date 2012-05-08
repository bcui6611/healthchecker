
import stats_buffer
import util

class ExecSQL:
    def run(self, accessor, stmt):
        result = accessor.execute(stmt)
        return result[0]

class NodeList:
    def run(self, accessor):
        result = []
        nodelist = accessor.execute("SELECT host, port, version, os, status FROM ServerNode", True)
        for node in nodelist:
            result.append({"ip": node[0], "port": node[1], "version" :node[2], "os": node[3], "status" : node[4]})
        
        return result

class BucketList:
    def run(self, accessor):
        result = []
        bucketlist = accessor.execute("SELECT name FROM Bucket", True)
        for bucket in bucketlist:
            result.append({"name": bucket[0]})
        
        return result

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
    {"name" : "NodeStatus",
     "ingredients" : [
        {
            "name" : "nodeList",
            "description" : "Node list",
            "type" : "pythonSQL",
            "code" : "NodeList",
        },
        {
            "name" : "numNodes",
            "description" : "Number of Nodes",
            "type" : "SQL",
            "stmt" : "SELECT count(*) FROM ServerNode",
            "code" : "ExecSQL",
        },
        {
            "name" : "numDownNodes",
            "description" : "Number of Down Nodes",
            "type" : "SQL",
            "stmt" : "SELECT count(*) FROM ServerNode WHERE status='down'",
            "code" : "ExecSQL",
        },
        {
            "name" : "numWarmupNodes",
            "description" : "Number of Warmup Nodes",
            "type" : "SQL",
            "stmt" : "SELECT count(*) FROM ServerNode WHERE status='warmup'",
            "code" : "ExecSQL",
        },
        {
            "name" : "numFailedOverNodes",
            "description" : "Number of Nodes failed over",
            "type" : "SQL",
            "stmt" : "SELECT count(*) FROM ServerNode WHERE clusterMembership != 'active'",
            "code" : "ExecSQL",
        },
      ],
      "clusterwise" : False,
      "nodewise" : True,
      "perNode" : False,
      "perBucket" : False,
    },
    {"name" : "NumberOfConnection",
    "ingredients" : [
        {
            "name" : "connectionTrend",
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
    {"name" : "OOMError",
     "ingredients" : [
        {
            "name" : "oomErrors",
            "description" : "OOM Errors",
            "counter" : "ep_oom_errors",
            "type" : "python",
            "scale" : "hour",
            "code" : "CalcTrend",
        },
        {
            "name" : "tempOomErrors",
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
            "name" : "overhead",
            "description" : "Overhead",
            "counter" : "ep_overhead",
            "type" : "python",
            "scale" : "hour",
            "code" : "CalcTrend",
        },
     ]
    },
    {"name" : "bucketList",
     "ingredients" : [
        {
            "name" : "bucketList",
            "description" : "Bucket list",
            "type" : "pythonSQL",
            "code" : "BucketList",
        },
     ],
     "nodewise" : True,
    },    
    
]


