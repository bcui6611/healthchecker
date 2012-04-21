import dbaccessor
import util

import cluster_stats
import bucket_stats
import diskqueue_stats
import node_stats

capsules = [
    (cluster_stats.ClusterCapsule, "cluster_stats"),
    #(bucket_stats.BucketCapsule, "bucket_stats"),
    (diskqueue_stats.DiskQueueCapsule, "diskqueue_stats"),
    (node_stats.NodeCapsule, "node_stats"),
]

cluster_symptoms = []
bucket_symptoms = []
node_symptoms = []

def  format_output(counter, result):
    if len(result) == 1:
        if counter.has_key("unit") and counter["unit"] == "GB":
            return util.pretty_float(result[0])
        else:
            return result[0]
    else:
        return result

class StatsAnalyzer:
    def __init__(self):
        self.accessor = dbaccessor.DbAccesor()

    def run_analysis(self):
        self.accessor.connect_db()
        #self.accessor.browse_db()

        for capsule, package_name in capsules:
            for pill in capsule:
            #print pill['name']
                for counter in pill['ingredients']:
                    if counter['type'] == 'SQL':
                        result = eval("{0}.{1}().run(self.accessor, \"{2}\")".format(package_name, counter['code'], counter['stmt']))
                    elif counter['type'] == 'pythonSQL':
                        result = eval("{0}.{1}().run(self.accessor)".format(package_name, counter['code']))
                    elif counter['type'] == 'python':
                        result = eval("{0}.{1}().run(counter)".format(package_name, counter['code']))
                    
                    if counter.has_key("unit") and counter["unit"] == "GB":
                        util.pretty_print({counter["description"] : result})
                    else:
                        util.pretty_print({counter["description"] : result})

                    #print counter
                    if pill.has_key("clusterwise") and pill["clusterwise"] :
                        if isinstance(result, dict):
                            if result.has_key("cluster"):
                                cluster_symptoms.append({counter["description"] : result["cluster"]})
                            else:
                                cluster_symptoms.append({counter["description"] : result})
                        else:
                            cluster_symptoms.append({counter["description"] : result})
                    if pill.has_key("perBucket") and pill["perBucket"] :
                        bucket_symptoms.append({counter["description"] :result})
                    if pill.has_key("perNode") and pill["perNode"] :
                        node_symptoms.append({counter["description"] :result})
        
        self.accessor.close()
        self.accessor.remove_db()
        
    def run_report(self):
        
        print "Cluster Overview"
        for symptom in cluster_symptoms:
            util.pretty_print(symptom)
        
        print "Bucket Metrics"
        for symptom in bucket_symptoms:
            util.pretty_print(symptom)
            
        print "Node Metrics"
        for symptom in node_symptoms:
            util.pretty_print(symptom)