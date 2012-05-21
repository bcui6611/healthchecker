import datetime
import dbaccessor
import util

import cluster_stats
import bucket_stats
import diskqueue_stats
import node_stats

import stats_buffer

from Cheetah.Template import Template

capsules = [
    (node_stats.NodeCapsule, "node_stats"),
    (cluster_stats.ClusterCapsule, "cluster_stats"),
    #(bucket_stats.BucketCapsule, "bucket_stats"),
    (diskqueue_stats.DiskQueueCapsule, "diskqueue_stats"),
]

globals = {
    "versions" : "1.0",
    "report_time" : datetime.datetime.now(),
    "cluster_health" : "ok",
}

node_list = {}
bucket_list = []
cluster_symptoms = {}
bucket_symptoms = {}
bucket_node_symptoms = {}
node_symptoms = {}
indicator_error = {}
indicator_warn = {}
node_disparate = {}

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
        self.accessor.browse_db()

        for bucket in stats_buffer.buckets.iterkeys():
            bucket_list.append(bucket)
            bucket_symptoms[bucket] = []
            bucket_node_symptoms[bucket] = {}

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
                    
                    #if counter.has_key("unit") and counter["unit"] == "GB":
                    #    util.pretty_print({counter["description"] : result})
                    #else:
                    #    util.pretty_print({counter["description"] : result})

                    #print counter
                    if pill.has_key("clusterwise") and pill["clusterwise"] :
                        if isinstance(result, dict):
                            if result.has_key("cluster"):
                                if counter.has_key("unit") and counter["unit"] == "GB":
                                    cluster_symptoms[counter["name"]] = {"description" : counter["description"], "value": util.humanize_bytes(result["cluster"])}
                                else:
                                    cluster_symptoms[counter["name"]] = {"description" : counter["description"], "value":result["cluster"]}
                            else:
                                cluster_symptoms[counter["name"]] = {"description" : counter["description"], "value":result}
                        else:
                            cluster_symptoms[counter["name"]] = {"description" : counter["description"], "value":result}
                    if pill.has_key("perBucket") and pill["perBucket"] :
                        #bucket_symptoms[counter["name"]] = {"description" : counter["description"], "value":result}
                        for bucket, values in result.iteritems():
                            if bucket == "cluster":
                                continue
                            for val in values:
                                if val[0] == "variance":
                                    continue
                                elif val[0] == "total":
                                    bucket_symptoms[bucket].append({"description" : counter["description"], "value" : values[-1][1]})
                                else:
                                    if bucket_node_symptoms[bucket].has_key(val[0]) == False:
                                        bucket_node_symptoms[bucket][val[0]] = []
                                    bucket_node_symptoms[bucket][val[0]].append({"description" : counter["description"], "value" : val[1]})

                    if pill.has_key("perNode") and pill["perNode"] :
                        node_symptoms[counter["name"]] = {"description" : counter["description"], "value":result}
                    if pill.has_key("nodewise") and pill["nodewise"]:
                        node_list[counter["name"]] = {"description" : counter["description"], "value":result}
                    
                    if pill.has_key("indicator") and pill["indicator"] :
                        if len(result) > 0:
                            for bucket,values in result.iteritems():
                                if values.has_key("error"):
                                    indicator_error[counter["name"]] = {"description" : counter["description"], "bucket": bucket, "value":values["error"]}
                                if values.has_key("warn"):
                                    indicator_warn[counter["name"]] = {"description" : counter["description"], "bucket": bucket, "value":values["warn"]}

                    if pill.has_key("nodeDisparate") and pill["nodeDisparate"] :
                        for bucket,values in result.iteritems():
                            if bucket == "cluster":
                                continue
                            for val in values:
                                if val[0] == "total":
                                    continue;
                                if val[0] == "variance" and val[1] != 0:
                                    node_disparate[counter["name"]] = {"description" : counter["description"], "bucket": bucket, "value":values}
                                    
        self.accessor.close()
        self.accessor.remove_db()
        
    def run_report(self):
        
        dict = {
            "globals" : globals,
            "cluster_symptoms" : cluster_symptoms,
            "bucket_symptoms" : bucket_symptoms,
            "bucket_node_symptoms" : bucket_node_symptoms,
            "node_symptoms" : node_symptoms,
            "node_list" : node_list,
            "bucket_list" : bucket_list,
            "indicator_warn" : indicator_warn,
            "indicator_error" : indicator_error,
        }
        
        debug = True
        if debug:
            print "Nodelist Overview"
            util.pretty_print(node_list)
            
            print "Cluster Overview"
            util.pretty_print(cluster_symptoms)
  
            print "Bucket Metrics"
            util.pretty_print(bucket_symptoms)

            print "Bucket Node Metrics"
            util.pretty_print(bucket_node_symptoms)
        
            print "Key indicators"
            util.pretty_print(indicator_error)
            util.pretty_print(indicator_warn)
        
            print "Node disparate"
            util.pretty_print(node_disparate)
        #print Template(file="report-htm.tmpl", searchList=[dict])