{
    "name" : "self",
    "desc" : "kv use case testing with rebalance",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "data_access",
                    "desc" :  "rebalance stage",
                    "workload" : ["s:30,u:20,g:50,d:0,coq:defaultph1keys,ccq:defaultph1keys,ops:10000",
                    "runtime" : 120
                }
        }
}
