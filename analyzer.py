import cluster_stats
import diskqueue_stats
import dbaccessor

class StatsAnalyzer:
    def __init__(self):
        self.accessor = dbaccessor.DbAccesor()

    def run_analysis(self):
        self.accessor.connect_db()
        self.accessor.browse_db()

        #print cluster_stats.ClusterCapsule
        for pill in cluster_stats.ClusterCapsule:
            #print pill['name']
            for counter in pill['ingredients']:
                if counter['type'] == 'SQL':
                    result = self.accessor.execute(counter['code'])
                    print counter["description"], ":", result[0]
                elif counter['type'] == 'python':
                    result = eval("cluster_stats.{0}().run(self.accessor)".format(counter['code']))
                    print counter["description"], ": ", result

        self.accessor.close()
        self.accessor.remove_db()