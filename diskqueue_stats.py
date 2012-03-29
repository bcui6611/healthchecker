import dbaccessor

class DiskQueue:
    def run(self, accessor):
        queue_size = accessor.execute("SELECT sum(diskWriteQueue) FROM BucketOps")
        total = accessor.execute("SELECT count(*) FROM BucketOps")
        disk_queue_avg = queue_size[0] / total[0]
        return (disk_queue_avg)

class TotalOps:
    def run(self, accessor):
        ops = accessor.execute("SELECT sum(getOps), sum(setOps), sum(delOps) FROM BucketOps")
        total = accessor.execute("SELECT count(*) FROM BucketOps")
        get_avg = ops[0] / total[0]
        set_avg = ops[1] / total[0]
        del_avg = ops[2] / total[0]
        total_avg = get_avg + set_avg + del_avg
        return (total_avg, get_avg / total_avg * 100, set_avg / total_avg * 100, del_avg / total_avg * 100)

DiskQueueCapsule = [
    {"name" : "Disk Queue Diagnosis",
     "ingredients" : [
        {
            "description" : "Avg Disk queue length",
            "type" : "python",
            "code" : "DiskQueue",
            "threshold" : {
                "low" : 50000000,
                "high" : 1000000000
            }
        },
        {
            "description" : "Total OPS",
            "type" : "python",
            "code" : "TotalOps"
        }
     ]
    } 
]