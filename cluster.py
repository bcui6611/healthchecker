
ClusterCapsule = {
    "pill" : { "name" : "Node Status",
           "ingredients" : {
                "counter" : {
                    "Descripition" : "Number of Nodes",
                    "Code" : "SELECT count(*) FROM ServerNode"
                },
                "counter" : {
                    "Descripition" : "Number of Down Nodes",
                    "Code" : "SELECT count(*) FROM ServerNode WHERE status='down'"
                },
                "counter" : {
                    "Description" : "Number of Warmup Nodes",
                    "Code" : "SELECT count(*) FROM ServerNode WHERE status='warmup'"
                }
            }
        }
}
