from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from xdcrbasetests import XDCRReplicationBaseTest
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection
from random import randrange

import time

#Assumption that at least 2 nodes on every cluster
#TODO fail the tests if this condition is not met
class bidirectional(XDCRReplicationBaseTest):
    def setUp(self):
        super(bidirectional, self).setUp()

        self.gen_create2 = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self._num_items)
        self.gen_delete2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size,
            start=int((self._num_items) * (float)(100 - self._percent_delete) / 100), end=self._num_items)
        self.gen_update2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, start=0,
            end=int(self._num_items * (float)(self._percent_update) / 100))

    def tearDown(self):
        super(bidirectional, self).tearDown()

    def _async_update_delete_data(self):
        self._log.info("The tasks:-")
        tasks = []
        #Setting up doc-ops at source nodes
        if self._doc_ops is not None:
            # allows multiple of them but one by one on either of the clusters
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires,
                                                          batch_size=1, pause_secs=5, timeout_secs=180))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0,
                                                          batch_size=1, pause_secs=5, timeout_secs=180))
            time.sleep(self._timeout / 6)
        if self._doc_ops_dest is not None:
            if "update" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_update2, "update", self._expires,
                                                          batch_size=1, pause_secs=5, timeout_secs=180))
            if "delete" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0,
                                                          batch_size=1, pause_secs=5, timeout_secs=180))
            time.sleep(self._timeout / 6)
        for task in tasks:
            task.result()


    """Bidirectional replication between two clusters(currently), create-updates-deletes on DISJOINT sets on same bucket."""
    # TODO fix exit condition on mismatch error, to check for a range instead of exiting on 1st mismatch
    def load_with_ops(self):
        self._modify_src_data()

        # Setting up doc_ops_dest at destination nodes
        if self._doc_ops_dest is not None:
        # allows multiple of them but one by one
            if "create" in self._doc_ops_dest:
                self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                                       batch_size=1, pause_secs=5, timeout_secs=180)
            if "update" in self._doc_ops_dest:
                self._load_all_buckets(self.dest_master, self.gen_update2, "update", self._expires,
                                       batch_size=1, pause_secs=5, timeout_secs=180)
            if "delete" in self._doc_ops_dest:
                self._load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0,
                                       batch_size=1, pause_secs=5, timeout_secs=180)

        time.sleep(self._timeout)
        self._wait_for_stats_all_buckets(self.src_nodes)
        self._wait_for_stats_all_buckets(self.dest_nodes)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.verify_results(verify_src=True)

    """Bidirectional replication between two clusters(currently), create-updates-deletes on DISJOINT sets on same bucket.
    Here running incremental load on both cluster1 and cluster2 as specified by the user/conf file"""

    def load_with_async_ops(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)

        time.sleep(self._timeout)
        self._async_update_delete_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.verify_results(verify_src=True)

    """Testing Bidirectional load( Loading at source/destination). Failover node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """
    def load_with_async_ops_and_joint_sets(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)

        time.sleep(self._timeout * 2)
        tasks = []
        if "update" in self._doc_ops:
            tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires,
                                                      batch_size=1, pause_secs=5, timeout_secs=180))
        if "update" in self._doc_ops_dest:
            tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_update, "update", self._expires,
                                                      batch_size=1, pause_secs=5, timeout_secs=180))
        if "delete" in self._doc_ops:
            tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0,
                                                      batch_size=1, pause_secs=5, timeout_secs=180))
        if "delete" in self._doc_ops_dest:
            tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete, "delete", 0,
                                                      batch_size=1, pause_secs=5, timeout_secs=180))

        time.sleep(self._timeout / 2)
        for task in tasks:
            task.result()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.verify_results(verify_src=True)

    def load_with_async_ops_with_warmup(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)

        time.sleep(self._timeout)
        #warmup
        warmupnodes = []
        if self._warmup is not None:
            if "source" in self._warmup:
                warmupnodes.append(self.src_nodes[randrange(1, len(self.src_nodes))])
            if "destination" in self._warmup:
                warmupnodes.append(self.dest_nodes[randrange(1, len(self.dest_nodes))])
        for node in warmupnodes:
            self.do_a_warm_up(node)
        time.sleep(self._timeout / 2)

        self._async_update_delete_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        time.sleep(self._timeout)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results(verify_src=True)

    def load_with_async_ops_with_warmup_master(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)

        time.sleep(self._timeout)
        #warmup
        warmupnodes = []
        if self._warmup is not None:
            if "source" in self._warmup:
                warmupnodes.append(self.src_master)
            if "destination" in self._warmup:
                warmupnodes.append(self.src_master)
        for node in warmupnodes:
            self.do_a_warm_up(node)
        time.sleep(self._timeout / 2)

        self._async_update_delete_data()

        time.sleep(self._timeout / 2)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results(verify_src=True)

    def load_with_async_ops_and_joint_sets_with_warmup(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)

        time.sleep(self._timeout)
        #warmup
        warmupnodes = []
        if self._warmup is not None:
            if "source" in self._warmup:
                warmupnodes.append(self.src_nodes[randrange(1, len(self.src_nodes))])
            if "destination" in self._warmup:
                warmupnodes.append(self.dest_nodes[randrange(1, len(self.dest_nodes))])
        for node in warmupnodes:
            self.do_a_warm_up(node)
        time.sleep(self._timeout / 2)

        self._async_update_delete_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        time.sleep(self._timeout / 2)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results(verify_src=True)

    def load_with_async_ops_and_joint_sets_with_warmup_master(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                                   batch_size=1, pause_secs=5, timeout_secs=180)

        time.sleep(self._timeout)
        #warmup
        warmupnodes = []
        if self._warmup is not None:
            if "source" in self._warmup:
                warmupnodes.append(self.src_master)
            if "destination" in self._warmup:
                warmupnodes.append(self.src_master)
        for node in warmupnodes:
            self.do_a_warm_up(node)
        time.sleep(self._timeout / 2)

        self._async_update_delete_data()

        time.sleep(self._timeout / 2)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results(verify_src=True)

    def load_with_failover(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        time.sleep(self._timeout)

        if self._failover is not None:
            if "source" in self._failover and len(self.src_nodes) > 1:
                if len(self.src_nodes) > 1:
                    i = len(self.src_nodes) - 1
                    self._cluster_helper.failover(self.src_nodes, [self.src_nodes[i]])
                    self._log.info(
                            " Failing over Source Non-Master Node {0}:{1}".format(self.src_nodes[i].ip, self.src_nodes[i].port))
                    self._cluster_helper.rebalance(self.src_nodes, [], [self.src_nodes[i]])
                    self.src_nodes.remove(self.src_nodes[i])
                else:
                    self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                            len(self.src_nodes)))
            if "destination" in self._failover and len(self.dest_nodes) > 1:
                if len(self.dest_nodes) > 1:
                    i = len(self.dest_nodes) - 1
                    self._cluster_helper.failover(self.dest_nodes, [self.dest_nodes[i]])
                    self._log.info(" Failing over Destination Non-Master Node {0}:{1}".format(self.dest_nodes[i].ip,
                                                                                              self.dest_nodes[i].port))
                    self._cluster_helper.rebalance(self.dest_nodes, [], [self.dest_nodes[i]])
                    self.dest_nodes.remove(self.dest_nodes[i])
                else:
                    self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                            len(self.dest_nodes)))

        time.sleep(self._timeout / 2)

        self._async_update_delete_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        time.sleep(self._timeout)

        self.verify_results(verify_src=True)

    def load_with_failover_then_add_back(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        time.sleep(self._timeout)

        if self._failover is not None:
            if "source" in self._failover and len(self.src_nodes) > 1:
                if len(self.src_nodes) > 1:
                    i = len(self.src_nodes) - 1
                    self._log.info(
                            " Failing over Source Non-Master Node {0}:{1}".format(self.src_nodes[i].ip, self.src_nodes[i].port))
                    self._cluster_helper.failover(self.src_nodes, [self.src_nodes[i]])
                    self._log.info(" Add back Source Non-Master Node {0}:{1}".format(self.src_nodes[i].ip,
                                                                                     self.src_nodes[i].port))
                    self.adding_back_a_node(self.src_master, self.src_nodes[i])
                    self._cluster_helper.rebalance(self.src_nodes, [], [])
                    self.src_nodes.remove(self.src_nodes[i])
                else:
                    self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                                    len(self.src_nodes)))
            if "destination" in self._failover and len(self.dest_nodes) > 1:
                if len(self.dest_nodes) > 1:
                    i = len(self.dest_nodes) - 1
                    self._log.info(" Failing over Destination Non-Master Node {0}:{1}".format(self.dest_nodes[i].ip,
                                                                                              self.dest_nodes[i].port))
                    self._cluster_helper.failover(self.dest_nodes, [self.dest_nodes[i]])
                    self._log.info(" Add back Destination Non-Master Node {0}:{1}".format(self.dest_nodes[i].ip,
                                                                                          self.dest_nodes[i].port))
                    self.adding_back_a_node(self.dest_master, self.dest_nodes[i])
                    self._cluster_helper.rebalance(self.dest_nodes, [], [])
                    self.dest_nodes.remove(self.dest_nodes[i])
                else:
                    self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                                    len(self.dest_nodes)))

        time.sleep(self._timeout / 2)

        self._async_update_delete_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        time.sleep(self._timeout)

        self.verify_results(verify_src=True)

    def load_with_failover_master(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        time.sleep(self._timeout)

        if self._failover is not None:
            if "source" in self._failover  and len(self.src_nodes) > 1:
                self._log.info(" Failing over Source Master Node {0}".format(self.src_master.ip))
                self._cluster_helper.failover(self.src_nodes, [self.src_master])
                self._log.info(" Rebalance out Source Master Node {0}".format(self.src_master.ip))
                self._cluster_helper.rebalance(self.src_nodes, [], [self.src_master])
                self.src_nodes.remove(self.src_master)
                self.src_master = self.src_nodes[0]
            elif "source" in self._failover and len(self.src_nodes) <= 1:
                self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                                len(self.src_nodes)))

            if "destination" in self._failover  and len(self.dest_nodes) > 1:
                self._log.info(" Failing over Destination Master Node {0}".format(self.dest_master.ip))
                self._cluster_helper.failover(self.dest_nodes, [self.dest_master])
                self._log.info(" Rebalance out Destination Master Node {0}".format(self.dest_master.ip))
                self._cluster_helper.rebalance(self.dest_nodes, [], [self.dest_master])
                self.dest_nodes.remove(self.dest_master)
                self.dest_master = self.dest_nodes[0]
            elif "destination" in self._failover and len(self.dest_nodes) <= 1:
                self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                                len(self.dest_nodes)))

        time.sleep(self._timeout / 2)

        self._async_update_delete_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
        time.sleep(self._timeout * 5)
        self.verify_results(verify_src=True)

    """Replication with compaction ddocs and view queries on both clusters.

    This test begins by loading a given number of items on both clusters.
    It creates _num_views as development/production view with default
    map view funcs(_is_dev_ddoc = True by default) on both clusters.
    Then we disabled compaction for ddoc on src cluster. While we don't reach
    expected fragmentation for ddoc on src cluster we update docs and perform
    view queries for all views. Then we start compaction when fragmentation
    was reached fragmentation_value. When compaction was completed we perform
    a full verification: wait for the disk queues to drain
    and then verify that there has been no data loss on both clusters."""
    def replication_with_ddoc_compaction(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                               batch_size=100, pause_secs=5, timeout_secs=180)
        self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                               batch_size=100, pause_secs=5, timeout_secs=180)
        time.sleep(self._timeout)

        src_buckets = self._get_cluster_buckets(self.src_master)
        for bucket in src_buckets:
            views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[self._is_dev_ddoc]

        query = {"full_set" : "true", "stale" : "false"}

        tasks = []
        tasks = self.async_create_views(self.src_master, ddoc_name, views, self.default_bucket_name)
        tasks += self.async_create_views(self.dest_master, ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self._poll_timeout)
        self.disable_compaction()
        fragmentation_monitor = self._cluster_helper.async_monitor_view_fragmentation(self.src_master,
                         prefix + ddoc_name, self.fragmentation_value, "default")
        # generate load until fragmentation reached
        while fragmentation_monitor.state != "FINISHED":
            # update docs to create fragmentation
            self._load_all_buckets(self.src_master, self.gen_update, "update", self._expires)
            for view in views:
                # run queries to create indexes
                self._cluster_helper.query_view(self.src_master, prefix + ddoc_name, view.name, query)
                self._cluster_helper.query_view(self.dest_master, prefix + ddoc_name, view.name, query)
        fragmentation_monitor.result()

        compaction_task = self._cluster_helper.async_compact_view(self.src_master, prefix + ddoc_name, 'default')

        result = compaction_task.result()
        self.assertTrue(result)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.verify_results(verify_src=True)


    def replication_with_view_queries_and_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        time.sleep(self._timeout)

        src_buckets = self._get_cluster_buckets(self.src_master)
        dest_buckets = self._get_cluster_buckets(self.src_master)
        for bucket in src_buckets:
            views = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
        for bucket in src_buckets:
            views_dest = self.make_default_views(bucket.name, self._num_views, self._is_dev_ddoc)
        ddoc_name = "ddoc1"
        prefix = ("", "dev_")[self._is_dev_ddoc]

        query = {"full_set" : "true", "stale" : "false"}

        tasks = []
        tasks = self.async_create_views(self.src_master, ddoc_name, views, self.default_bucket_name)
        tasks += self.async_create_views(self.dest_master, ddoc_name, views, self.default_bucket_name)
        for task in tasks:
            task.result(self._poll_timeout)

        #self._async_update_delete_data()
        #self._cluster_helper.query_view(self.src_master, prefix + ddoc_name, view.name, query)
        #self._cluster_helper.query_view(self.dest_master, prefix + ddoc_name, view.name, query)

        tasks = []
        #Setting up doc-ops at source nodes
        if self._doc_ops is not None:
            # allows multiple of them but one by one on either of the clusters
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires,
                                                          batch_size=1, pause_secs=5, timeout_secs=180))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0,
                                                          batch_size=1, pause_secs=5, timeout_secs=180))
            time.sleep(5)
        if self._doc_ops_dest is not None:
            if "update" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_update2, "update", self._expires,
                                                          batch_size=1, pause_secs=5, timeout_secs=180))
            if "delete" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0,
                                                          batch_size=1, pause_secs=5, timeout_secs=180))
            time.sleep(5)
        while True:
            for view in views:
                self._cluster_helper.query_view(self.src_master, prefix + ddoc_name, view.name, query)
                self._cluster_helper.query_view(self.dest_master, prefix + ddoc_name, view.name, query)
            for task in tasks:
                if task.state != "FINISHED":
                    continue;
            break;

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        tasks = []
        for view in views:
            tasks.append(self._cluster_helper.async_query_view(self.src_master, prefix + ddoc_name, view.name, query, src_buckets[0].kvs[1].__len__()))
            tasks.append(self._cluster_helper.async_query_view(self.dest_master, prefix + ddoc_name, view.name, query, dest_buckets[0].kvs[1].__len__()))

        for task in tasks:
            task.result(self._poll_timeout)

        self.verify_results(verify_src=True)


    """Replication with disabled/enabled ddoc compaction on both clusters.

    This test begins by loading a given number of items on both clusters.
    Then we disabled or enabled compaction on both clusters( set via params).
    Then we mutate and delete data on clusters 3 times. After deletion we recreate
    deleted items. When data was changed 3 times we perform
    a full verification: wait for the disk queues to drain
    and then verify that there has been no data loss on both clusters."""
    def replication_with_disabled_ddoc_compaction(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        time.sleep(self._timeout)

        disable_src_comp = self._input.param("disable_src_comp", True)
        disable_dest_comp = self._input.param("disable_dest_comp", True)
        if disable_src_comp:
            self.disable_compaction(self.src_master)
        if disable_dest_comp:
            self.disable_compaction(self.dest_master)

        # perform doc's ops 3 times to increase rev number
        for i in range(3):
            time.sleep(30)
            self._async_update_delete_data()
            tasks = []
            #restore deleted items
            if self._doc_ops is not None:
                if "delete" in self._doc_ops:
                    tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "create", 0))
            if self._doc_ops_dest is not None:
                if "delete" in self._doc_ops_dest:
                    tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete2, "create", 0))
            time.sleep(5)
            for task in tasks:
                task.result()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.verify_results(verify_src=True)

    def replication_while_rebooting_a_non_master_destination_node(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0,
                               batch_size=1, pause_secs=5, timeout_secs=180)
        self._async_update_delete_data()
        time.sleep(self._timeout)

        i = len(self.dest_nodes) - 1
        shell = RemoteMachineShellConnection(self.dest_nodes[i])
        o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        i = len(self.src_nodes) - 1
        shell = RemoteMachineShellConnection(self.src_nodes[i])
        o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        time.sleep(self._timeout * 2)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.verify_results(verify_src=True)
