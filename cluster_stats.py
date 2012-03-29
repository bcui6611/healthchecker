import dbaccessor

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
        ops = accessor.execute("SELECT sum(getOps), sum(setOps), sum(delOps) FROM BucketOps")
        total = accessor.execute("SELECT count(*) FROM BucketOps")
        get_avg = ops[0] / total[0]
        set_avg = ops[1] / total[0]
        del_avg = ops[2] / total[0]
        total_avg = get_avg + set_avg + del_avg
        return (get_avg / total_avg * 100, set_avg / total_avg * 100, del_avg / total_avg * 100)

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
            "type" : "python",
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
            "code" : "OpsRatio"
        },
     ]
    }
]


