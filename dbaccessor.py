try:
    import sqlite3
except:
    print "healthchecker requires python version 2.6 or greater"
    sys.exit(1)
import os

class DbAccesor:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.db_file = "perfmon.db"

    def connect_db(self):
        self.conn = sqlite3.connect(self.db_file)
        self.cursor = self.conn.cursor()

    def close(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
        self.conn = None
        self.cursor = None

    def remove_db(self):
        os.remove(self.db_file)

    def create_databases(self):
        self.cursor.execute(""" CREATE TABLE IF NOT EXISTS ServerNode (
                serverId INTEGER PRIMARY KEY, 
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                status TEXT,
                portDirect INTEGER,
                portProxy INTEGER,
                clusterMembership INTEGER,
                os TEXT,
                uptime INTEGER,
                version TEXT)""")

        self.cursor.execute(""" CREATE TABLE IF NOT EXISTS DiskInfo (
                diskInfoId INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                sizeBytes INTEGER,
                usagePercent INTEGER,
                serverId INTEGER,
                FOREIGN KEY(serverId) REFERENCES ServerNode(serverId))""") 

        self.cursor.execute(""" CREATE TABLE IF NOT EXISTS MemoryInfo (
                memoryInfoId INTEGER PRIMARY KEY,
                allocated INTEGER,
                reserved INTEGER,
                free INTEGER,
                quota INTEGER,
                total INTEGER,
                serverId INTEGER,
                FOREIGN KEY(serverId) REFERENCES ServerNode(serverId))""") 
    
        self.cursor.execute(""" CREATE TABLE IF NOT EXISTS StorageInfo (
                storageInfoId INTEGER PRIMARY KEY,
                type TEXT,
                free REAL,
                quotaTotal REAL,
                total REAL,
                used REAL,
                usedbyData REAL,
                serverId INTEGER,
                FOREIGN KEY(serverId) REFERENCES ServerNode(serverId))""") 

        self.cursor.execute(""" CREATE TABLE IF NOT EXISTS SystemStats (
                id INTEGER PRIMARY KEY,
                cpuUtilization REAL,
                swapTotal REAL,
                swapUsed  REAL,
                currentItems INTEGER,
                currentItemsTotal INTEGER,
                replicaCurrentItems INTEGER,
                serverId INTEGER,
                FOREIGN KEY(serverId) REFERENCES ServerNode(serverId))""")

        self.cursor.execute(""" CREATE TABLE IF NOT EXISTS Bucket (
                bucketId INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                authType TEXT,
                saslPassword TEXT,
                numReplica INTEGER,
                ramQuota REAL)""")
    
        self.cursor.execute(""" CREATE TABLE IF NOT EXISTS BucketStats (
                id INTEGER PRIMARY KEY,
                diskUsed REAL,
                memUsed  REAL,
                diskFetch INTEGER,
                quotaPercentUsed REAL,
                opsPerSec INTEGER,
                itemCount INTEGER,
                bucketId INTEGER,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(bucketId) REFERENCES Bucket(bucketId))""")
        
        self.cursor.execute(""" CREATE TABLE IF NOT EXISTS BucketOps (
                id INTEGER PRIMARY KEY,
                getOps REAL,
                setOps REAL,
                delOps REAL,
                diskWriteQueue REAL,
                bucketId INTEGER,
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(bucketId) REFERENCES Bucket(bucketId))""")

    def create_or_update_node(self, host, port, status):
        sqlstmt = """INSERT OR REPLACE INTO ServerNode (host,port,status) 
                 VALUES( '{0}', {1}, '{2}' )"""
        self.cursor.execute(sqlstmt.format(host, port, status))
        return self.cursor.lastrowid

    def process_node_stats(self, nodeId, nodeInfo):
        sqlstmt = """ UPDATE ServerNode
                  SET portDirect={0}, portProxy={1}, clusterMembership='{2}',
                      os='{3}', uptime={4}, version='{5}'
                  WHERE serverId = {6}"""
        self.cursor.execute(sqlstmt.format(nodeInfo['ports']['direct'],
                   nodeInfo['ports']['proxy'],
                   nodeInfo['clusterMembership'],
                   nodeInfo['os'],
                   nodeInfo['uptime'],
                   nodeInfo['version'],
                   nodeId));

        #memory
        sqlstmt = """ INSERT OR REPLACE INTO MemoryInfo 
            (allocated, reserved, free, quota, total, serverId)
            VALUES({0}, {1}, {2}, {3}, {4}, {5})"""

        self.cursor.execute(sqlstmt.format(nodeInfo['mcdMemoryAllocated'],
                    nodeInfo['mcdMemoryReserved'],
                    nodeInfo['memoryFree'],
                    nodeInfo['memoryQuota'],
                    nodeInfo['memoryTotal'],
                    nodeId));

        #storageInfo
        sqlstmt = """ INSERT OR REPLACE INTO StorageInfo
            (type, free, quotaTotal, total, used, usedbyData, serverId)
            VALUES('{0}', {1}, {2}, {3}, {4}, {5}, {6})"""

        if nodeInfo['storageTotals'] is not None:
            hdd = nodeInfo['storageTotals']['hdd']
            if hdd is not None:
                self.cursor.execute(sqlstmt.format('hdd',
                        hdd['free'],
                        hdd['quotaTotal'],
                        hdd['total'],
                        hdd['used'],
                        hdd['usedByData'],
                        nodeId));
            ram = nodeInfo['storageTotals']['ram']
            if hdd is not None:
                self.cursor.execute(sqlstmt.format('ram',
                        hdd['free'],
                        hdd['quotaTotal'],
                        hdd['total'],
                        hdd['used'],
                        hdd['usedByData'],
                        nodeId));

        #system stats
        sqlstmt = """ INSERT OR REPLACE INTO SystemSTats 
            (cpuUtilization, swapTotal, swapUsed, currentItems, currentItemsTotal, replicaCurrentItems, serverId)
            VALUES({0}, {1}, {2}, {3}, {4}, {5}, {6})"""
        if nodeInfo['interestingStats'] is not None:
            if nodeInfo['interestingStats'].has_key('curr_items'):
                curr_items = nodeInfo['interestingStats']['curr_items']
            else:
                curr_items = 0
            if nodeInfo['interestingStats'].has_key('curr_items_tot'):
                curr_items_tot = nodeInfo['interestingStats']['curr_items_tot']
            else:
                curr_items_tot = 0
            if nodeInfo['interestingStats'].has_key('vb_replica_curr_items'):
                vb_rep_curr_items = nodeInfo['interestingStats']['vb_replica_curr_items']
            else:
                vb_rep_curr_items = 0
        else:
            curr_items = 0
            curr_items_tot = 0
            vb_rep_curr_items = 0
        self.cursor.execute(sqlstmt.format(nodeInfo['systemStats']['cpu_utilization_rate'],
                    nodeInfo['systemStats']['swap_total'],
                    nodeInfo['systemStats']['swap_used'],
                    curr_items,
                    curr_items_tot,
                    vb_rep_curr_items,
                    nodeId)); 

        return True

    def process_bucket(self, bucket):
        sqlstmt = """INSERT OR REPLACE INTO Bucket 
                    (name, type, authType, saslPassword, numReplica, ramQuota) 
                    VALUES('{0}', '{1}', '{2}', '{3}', {4}, {5})"""
        self.cursor.execute(sqlstmt.format(bucket['name'],
                    bucket['bucketType'],
                    bucket['authType'],
                    bucket['saslPassword'],
                    bucket['replicaNumber'],
                    bucket['quota']['ram']))
        bucketId = self.cursor.lastrowid

        sqlstmt = """INSERT INTO BucketStats 
            (diskUsed, memUsed, diskFetch, quotaPercentUsed, opsPerSec, itemCount, bucketId)
            VALUES({0}, {1}, {2}, {3}, {4}, {5}, {6})"""
        bucketStats = bucket['basicStats']
        self.cursor.execute(sqlstmt.format(bucketStats['diskUsed'],
            bucketStats['memUsed'],
            bucketStats['diskFetches'],
            bucketStats['quotaPercentUsed'],
            bucketStats['opsPerSec'],
            bucketStats['itemCount'],
            bucketId))
        return (bucket['name'], bucketId)

    def process_bucket_stats(self, bucket_id, json):
        sqlstmt = """INSERT OR REPLACE INTO BucketOps
                    (getOps, setOps, delOps, diskWriteQueue, bucketId)
                    VALUES({0}, {1}, {2}, {3}, {4})"""
        #print "op", json["op"]
        #print "op/sample", json["op"]["samples"]
        #print "op/sample/cmd_get", json["op"]["samples"]["cmd_get"]
        samples = json["op"]["samples"]
        for sample in samples.keys():
            print "-->",sample
        total_samples = json["op"]["samplesCount"]
        get_avg = sum(json["op"]["samples"]["cmd_get"]) / total_samples
        set_avg = sum(json["op"]["samples"]["cmd_set"]) / total_samples
        del_avg = sum(json["op"]["samples"]["delete_hits"]) / total_samples
        disk_write_queue_avg = sum(json["op"]["samples"]["disk_write_queue"]) / total_samples
        #print get_avg, set_avg, del_avg, disk_write_queue_avg
        self.cursor.execute(sqlstmt.format(get_avg, set_avg, del_avg, disk_write_queue_avg, bucket_id))

    def process_bucket_node_stats(self, bucket_id, node_name, stat, jason):
        sqlstmt = """INSERT OR REPLACE INTO BucketOps
                    (getOps, setOps, delOps, diskWriteQueue, bucketId)
                    VALUES({0}, {1}, {2}, {3}, {4})"""
        #print "op", json["op"]
        #print "op/sample", json["op"]["samples"]
        #print "op/sample/cmd_get", json["op"]["samples"]["cmd_get"]
        #samples = json["op"]["samples"]
        #for sample in samples.keys():
        #    print sample
        #total_samples = json["op"]["samplesCount"]
        
        #del_avg = sum(json["op"]["samples"]["delete_hits"]) / total_samples
        #disk_write_queue_avg = sum(json["op"]["samples"]["disk_write_queue"]) / total_samples
        #print get_avg, set_avg, del_avg, disk_write_queue_avg
        #self.cursor.execute(sqlstmt.format(get_avg, set_avg, del_avg, disk_write_queue_avg, bucket_id))

    def extract_result(self, rows):
        if rows is not None:
            for row in rows:
                return row
        else:
            return [0]

    def execute(self, stmt):
        self.cursor.execute(stmt)
        return self.extract_result(self.cursor.fetchall())

    def browse_table(self, table):
        stmt = "SELECT * from {0}"
        self.cursor.execute(stmt.format(table))
        rows = self.cursor.fetchall()
        for row in rows:
            print row

    def browse_db(self):
        self.browse_table("ServerNode")
        self.browse_table("MemoryInfo")
        self.browse_table("StorageInfo")
        self.browse_table("SystemStats")
        self.browse_table("Bucket")
        self.browse_table("BucketStats")
        self.browse_table("BucketOps")
