import sys
import unittest
import logger
import copy
import time
from datetime import datetime

from membase.api.rest_client import RestConnection, Bucket
from couchbase.cluster import Cluster
from couchbase.document import View
from TestInput import TestInputSingleton
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from mc_bin_client import MemcachedError
from couchbase.documentgenerator import BlobGenerator
from basetestcase import BaseTestCase


class preSet(unittest.TestCase):
    def setUp(self):
        try:
            self._log = logger.Logger.get_logger()
            self._input = TestInputSingleton.input
            self._init_parameters()
            self._cluster_helper = Cluster()
            self._log.info("==============  cbRecoveryTests setup was started for test #{0} {1} =============="\
                .format(self._case_number, self._testMethodName))
            if not self._input.param("skip_cleanup", False) and str(self.__class__).find('upgradeXDCR') == -1:
                self._cleanup_previous_setup()

            self._init_clusters()
            self._setup_topology()
            self._log.info("==============  cbRecoveryTests setup was finished for test #{0} {1} =============="\
                .format(self._case_number, self._testMethodName))

        except Exception as e:
            self._log.error(e.message)
            self._log.error("Error while setting up the cluster: %s", sys.exc_info())
            self._cleanup_broken_setup()
            raise

    def tearDown(self):
        try:
            self._log.info("==============  cbRecoveryTests stats for test #{0} {1} =============="\
                    .format(self._case_number, self._testMethodName))
            #super(preSet, self).tearDown()
            self._log.info("==============  cbRecoveryTests cleanup was started for test #{0} {1} =============="\
                .format(self._case_number, self._testMethodName))
            for _, clusters in self._clusters_dic.items():
                rest = RestConnection(clusters[0])
                rest.remove_all_remote_clusters()
                rest.remove_all_replications()

            self._do_cleanup()
            self._log.info("==============  cbRecoveryTests cleanup was finished for test #{0} {1} =============="\
                .format(self._case_number, self._testMethodName))
            #self.tearDown()
        finally:
            self._cluster_helper.shutdown()

    def _cleanup_previous_setup(self):
        for _, clusters in self._clusters_dic.items():
            rest = RestConnection(clusters[0])
            rest.remove_all_remote_clusters()
            rest.remove_all_replications()
        self._do_cleanup()

    def _do_cleanup(self):
        for key in self._clusters_keys_olst:
            nodes = self._clusters_dic[key]
            self._log.info("cleanup cluster{0}: {1}".format(key + 1, nodes))
            for node in nodes:
                BucketOperationHelper.delete_all_buckets_or_assert([node], self)
                ClusterOperationHelper.cleanup_cluster([node], self)
                ClusterOperationHelper.wait_for_ns_servers_or_assert([node], self)

    def _cleanup_broken_setup(self):
        try:
            self.tearDown()
        except:
            self._log.info("Error while cleaning broken setup.")

    def _init_parameters(self):
        self._log.info("Initializing input parameters started...")
        self._clusters_dic = self._input.clusters  # clusters is declared as dic in TestInput which is unordered.
        self._clusters_keys_olst = range(
            len(self._clusters_dic))
        self._cluster_counter_temp_int = 0
        self._cluster_names_dic = self._get_cluster_names()
        self._servers = self._input.servers
        self._disabled_consistent_view = self._input.param("disabled_consistent_view", True)
        self.buckets = []

        self._default_bucket = self._input.param("default_bucket", True)

        self._standard_buckets = self._input.param("standard_buckets", 0)
        self._sasl_buckets = self._input.param("sasl_buckets", 0)

        if self._default_bucket:
            self.default_bucket_name = "default"

        self._num_replicas = self._input.param("replicas", 1)
        self._num_items = self._input.param("items", 1000)
        self._value_size = self._input.param("value_size", 256)
        self._dgm_run_bool = self._input.param("dgm_run", False)
        self._mem_quota_int = 0  # will be set in subsequent methods

        self._rdirection = self._input.param("rdirection","unidirection")

        self._failover_count = self._input.param("fail_count", 0)
        self._add_count = self._input.param("add_count", 0) 

        self._doc_ops = self._input.param("doc-ops", None)
        if self._doc_ops is not None:
            self._doc_ops = self._doc_ops.split("-")
        self._doc_ops_dest = self._input.param("doc-ops-dest", None)
        if self._doc_ops_dest is not None:
            self._doc_ops_dest = self._doc_ops_dest.split("-")

        self._case_number = self._input.param("case_number", 0)
        self._expires = self._input.param("expires", 0)
        self._timeout = self._input.param("timeout", 60)
        self._percent_update = self._input.param("upd", 30)
        self._percent_delete = self._input.param("del", 30)
        self._failover = self._input.param("failover", None)

        self.gen_create = BlobGenerator('loadOne', 'loadOne', self._value_size, end=self._num_items)
        self.gen_delete = BlobGenerator('loadOne', 'loadOne-', self._value_size,
            start=int((self._num_items) * (float)(100 - self._percent_delete) / 100), end=self._num_items)
        self.gen_update = BlobGenerator('loadOne', 'loadOne-', self._value_size, start=0,
            end=int(self._num_items * (float)(self._percent_update) / 100))

        if "bidirection" in self._rdirection:
            self.gen_create = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self._num_items)
            self.gen_delete = BlobGenerator('loadTwo', 'loadTwo-', self._value_size,
                start=int((self._num_items) * (float)(100 - self._percent_delete) / 100), end=self._num_items)
            self.gen_update = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, start=0,
                end=int(self._num_items * (float)(self._percent_update) / 100))

        self.ord_keys = self._clusters_keys_olst
        self.ord_keys_len = len(self.ord_keys)

        self._source = copy.copy(self._clusters_dic[0])
        self._source_master = self._source[0]

        self._sink = copy.copy(self._clusters_dic[1])
        self._sink_master = self._sink[0]

        self._spares = self._get_floating_servers()

        self._optimistic_xdcr = self._input.param("optimistic_xdcr", True)
        if self._source_master.ip != self._sink_master.ip:       #Only if it's not a cluster_run
            if self._optimistic_xdcr:
                self.set_environ_param('XDCR_LATENCY_OPTIMIZATION',True)

        self._cluster_state_arr = []
        self._log.info("Initializing input parameters completed.")   

    def set_environ_param(self, _parameter, value):
        self._log.info("Setting {0} to {1} ..".format(_parameter, value))
        for server in self._source:
            shell = RemoteMachineShellConnection(server)
            shell.set_environment_variable(_parameter, value)
        if "bidirection" in self._rdirection:
            for server in self._sink:
                shell = RemoteMachineShellConnection(server)
                shell.set_environment_variable(_parameter, value)

    def _get_floating_servers(self):
        cluster_nodes = []
        floating_servers = copy.copy(self._servers)

        for key, node in self._clusters_dic.items():
            cluster_nodes.extend(node)

        for c_node in cluster_nodes:
            for node in floating_servers:
                if node.ip in str(c_node) and node.port in str(c_node):
                    floating_servers.remove(node)

        return floating_servers

    def _init_clusters(self):
        for key in self._clusters_keys_olst:
            self._setup_cluster(self._clusters_dic[key])

    def _get_cluster_names(self):
        cs_names = {}
        for key in self._clusters_keys_olst:
            cs_names[key] = "cluster{0}".format(self._cluster_counter_temp_int)
            self._cluster_counter_temp_int += 1
        return cs_names

    def _setup_cluster(self, nodes):
        self._init_nodes(nodes)
        #self._cluster_helper.async_rebalance(nodes, nodes[1:], []).result()
        master = nodes[0]
        rest = RestConnection(master)
        for node in nodes[1:]:
            rest.add_node(master.rest_username, master.rest_password,
                          node.ip, node.port)
        servers = rest.node_statuses()
        rest.rebalance(otpNodes=[node.id for node in servers], ejectedNodes=[])
        time.sleep(self._timeout / 4)
        self._create_buckets(nodes)

    def _init_nodes(self, nodes):
        _tasks = []
        for node in nodes:
            _tasks.append(self._cluster_helper.async_init_node(node, True))
        for task in _tasks:
            mem_quota_node = task.result()
            if mem_quota_node < self._mem_quota_int or self._mem_quota_int == 0:
                self._mem_quota_int = mem_quota_node

#
#    def _create_sasl_buckets(self, server, num_buckets, server_id, bucket_size):
#        bucket_tasks = []
#        for i in range(num_buckets):
#            name = "sasl_bucket_" + str(i + 1)
#            bucket_tasks.append(self._cluster_helper.async_create_sasl_bucket(server, name, 'password',
#                                                                              bucket_size, self._num_replicas))
#            self.buckets.append(Bucket(name=name, authType="sasl", saslPassword="password",
#                                        num_replicas=self._num_replicas, bucket_size=bucket_size,
#                                        master_id=server_id))
#
#        for task in bucket_tasks:
#            task.result()
#
#    def _create_standard_buckets(self, server, num_buckets, server_id, bucket_size):
#        bucket_tasks = []
#        for i in range(num_buckets):
#            name = "standard_bucket_" + str(i + 1)
#            bucket_tasks.append(self._cluster_helper.async_create_standard_bucket(server, name,
#                                                                                  11214 + i,
#                                                                                  bucket_size,
#                                                                                  self._num_replicas))
#            self.buckets.append(Bucket(name=name, authType=None, saslPassword=None,
#                                        num_replicas=self._num_replicas, bucket_size=bucket_size,
#                                        port=11214 + i, master_id=server_id))
#
#        for task in bucket_tasks:
#            task.result()

    def _create_buckets(self, nodes):
        if self._dgm_run_bool:
            self._mem_quota_int = 256
        master_node = nodes[0]
        total_buckets = self._sasl_buckets + self._default_bucket + self._standard_buckets
        bucket_size = self._get_bucket_size(self._mem_quota_int, total_buckets)
        rest = RestConnection(master_node)
        master_id = rest.get_nodes_self().id

#
#        self._create_sasl_buckets(master_node, self._sasl_buckets, master_id, bucket_size)
#        self._create_standard_buckets(master_node, self._standard_buckets, master_id, bucket_size)

        if self._default_bucket:
            self._cluster_helper.create_default_bucket(master_node, bucket_size, self._num_replicas)
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                num_replicas=self._num_replicas, bucket_size=bucket_size, master_id=master_id))

    def _get_bucket_size(self, mem_quota, num_buckets, ratio=2.0 / 3.0):
        return int(ratio / float(num_buckets) * float(mem_quota))

    def _poll_for_condition(self, condition):
        timeout = self._poll_timeout
        interval = self._poll_interval
        num_itr = timeout / interval
        return self._poll_for_condition_rec(condition, interval, num_itr)

    def _poll_for_condition_rec(self, condition, sleep, num_itr):
        if num_itr == 0:
            return False
        else:
            if condition():
                return True
            else:
                self.sleep(sleep)
                return self._poll_for_condition_rec(condition, sleep, (num_itr - 1))

    def _get_cluster_buckets(self, master_server):
        rest = RestConnection(master_server)
        master_id = rest.get_nodes_self().id

        if master_id.find('es') != 0:

            #verify if node_ids were changed for cluster_run
            for bucket in self.buckets:
                if ("127.0.0.1" in bucket.master_id and "127.0.0.1" not in master_id) or \
                   ("localhost" in bucket.master_id and "localhost" not in master_id):
                    new_ip = master_id[master_id.index("@") + 1:]
                    bucket.master_id = bucket.master_id.replace("127.0.0.1", new_ip).\
                    replace("localhost", new_ip)

        return [bucket for bucket in self.buckets if bucket.master_id == master_id]

    """merge 2 different kv strores from different clsusters/buckets
       assume that all elements in the second kvs are more relevant.

    Returns:
            merged kvs, that we expect to get on both clusters
    """
    def merge_keys(self, kv_store_first, kv_store_second, kvs_num=1):
        valid_keys_first, deleted_keys_first = kv_store_first[kvs_num].key_set()
        valid_keys_second, deleted_keys_second = kv_store_second[kvs_num].key_set()

        for key in valid_keys_second:
            #replace the values for each key in first kvs if the keys are presented in second one
            if key in valid_keys_first:
                partition1 = kv_store_first[kvs_num].acquire_partition(key)
                partition2 = kv_store_second[kvs_num].acquire_partition(key)
                key_add = partition2.get_key(key)
                partition1.valid[key] = {"value"   : key_add["value"],
                           "expires" : key_add["expires"],
                           "flag"    : key_add["flag"]}
                kv_store_first[1].release_partition(key)
                kv_store_second[1].release_partition(key)
            #add keys/values in first kvs if the keys are presented only in second one
            else:
                partition1, num_part = kv_store_first[kvs_num].acquire_random_partition()
                partition2 = kv_store_second[kvs_num].acquire_partition(key)
                key_add = partition2.get_key(key)
                partition1.valid[key] = {"value"   : key_add["value"],
                           "expires" : key_add["expires"],
                           "flag"    : key_add["flag"]}
                kv_store_first[kvs_num].release_partition(num_part)
                kv_store_second[kvs_num].release_partition(key)
            #add condition when key was deleted in first, but added in second

        for key in deleted_keys_second:
            # the same keys were deleted in both kvs
            if key in deleted_keys_first:
                pass
            # add deleted keys to first kvs if the where deleted only in second kvs
            else:
                partition1 = kv_store_first[kvs_num].acquire_partition(key)
                partition2 = kv_store_second[kvs_num].acquire_partition(key)
                partition1.deleted[key] = partition2.get_key(key)
                if key in partition1.valid:
                    del partition1.valid[key]
                kv_store_first[kvs_num].release_partition(key)
                kv_store_second[kvs_num].release_partition(key)
            # return merged kvs, that we expect to get on both clusters
        return kv_store_first[kvs_num]

    def merge_buckets(self, source_master, sink_master, bidirection=True):
        self._log.info("merge buckets {0}->{1}, bidirection:{2}".format(source_master.ip, sink_master.ip, bidirection))
        self.do_merge_buckets(source_master, sink_master, bidirection)

    def do_merge_buckets(self, source_master, sink_master, bidirection):
        src_buckets = self._get_cluster_buckets(source_master)
        dest_buckets = self._get_cluster_buckets(sink_master)
        for src_bucket in src_buckets:
            for dest_bucket in dest_buckets:
                if src_bucket.name == dest_bucket.name:
                    if bidirection:
                        src_bucket.kvs[1] = self.merge_keys(src_bucket.kvs, dest_bucket.kvs, kvs_num=1)
                    dest_bucket.kvs[1] = src_bucket.kvs[1]

    def do_merge_bucket(self, source_master, sink_master, bidirection, bucket):
        src_buckets = self._get_cluster_buckets(source_master)
        dest_buckets = self._get_cluster_buckets(sink_master)
        for src_bucket in src_buckets:
            for dest_bucket in dest_buckets:
                if src_bucket.name == dest_bucket.name and bucket.name == src_bucket.name:
                    if bidirection:
                        src_bucket.kvs[1] = self.merge_keys(src_bucket.kvs, dest_bucket.kvs, kvs_num=1)
                    dest_bucket.kvs[1] = src_bucket.kvs[1]

        """Verify the stats at the destination cluster
        1. Data Validity check - using kvstore-node key-value check
        2. Item count check on source versus destination
        3. For deleted and updated items, check the CAS/SeqNo/Expiry/Flags for same key on source/destination
        * Make sure to call expiry_pager function to flush out temp items(deleted/expired items)"""
    def verify_xdcr_stats(self, source_nodes, sink_nodes, verify_src=False):
        timeout = 500
        if self._failover is not None:
            timeout *= 2

        #for verification src and dest clusters need more time
        if verify_src:
            timeout *= 3 / 2

        self._expiry_pager(self._source[0])
        self._expiry_pager(self._sink[0])
        end_time = time.time() + timeout
        if verify_src:
            self._log.info("and Verify xdcr replication stats at Source Cluster : {0}".format(self._source_master.ip))
            timeout = max(120, end_time - time.time())
            self._wait_for_stats_all_buckets(source_nodes, timeout=timeout)
        timeout = max(120, end_time - time.time())
        self._log.info("Verify xdcr replication stats at Destination Cluster : {0}".format(self._sink_master.ip))
        self._wait_for_stats_all_buckets(sink_nodes, timeout=timeout)
        if verify_src:
            timeout = max(120, end_time - time.time())
            self._verify_stats_all_buckets(source_nodes, timeout=timeout)
            timeout = max(120, end_time - time.time())
            self._verify_all_buckets(self._source_master)
        timeout = max(120, end_time - time.time())
        self._verify_stats_all_buckets(sink_nodes, timeout=timeout)
        timeout = max(120, end_time - time.time())
        self._verify_all_buckets(self._sink_master)

        errors_caught = 0
        if self._doc_ops is not None or self._doc_ops_dest is not None:
            if "update" in self._doc_ops or (self._doc_ops_dest is not None and "update" in self._doc_ops_dest):
                errors_caught = self._verify_revIds(self._source_master, self._sink_master, "update")

            if "delete" in self._doc_ops or (self._doc_ops_dest is not None and "delete" in self._doc_ops_dest):
                errors_caught = self._verify_revIds(self._source_master, self._sink_master, "delete")

        if errors_caught > 0:
            self.fail("Mismatches on Meta Information on xdcr-replicated items!")

    def verify_results(self, verify_src=False):
        if len(self.ord_keys) == 2:
            source_nodes = self.get_servers_in_cluster(self._source_master)
            sink_nodes = self.get_servers_in_cluster(self._sink_master)
            self.verify_xdcr_stats(source_nodes, sink_nodes, verify_src)
        else:
             # Checking replication at destination clusters when more then 2 clusters defined
             for cluster_num in self.ord_keys[1:]:
                 if dest_key_index == self.ord_keys_len:
                     break
                 self.sink_nodes = self._clusters_dic[cluster_num]
                 self.verify_xdcr_stats(self.source_nodes, self.sink_nodes, verify_src)

    def get_servers_in_cluster(self, member):
        nodes = [node for node in RestConnection(member).get_nodes()]
        servers = []
        cluster_run = len(set([server.ip for server in self._servers])) == 1
        for server in self._servers:
            for node in nodes:
                if (server.ip == str(node.ip) or cluster_run)\
                 and server.port == str(node.port):
                    servers.append(server)
        return servers

    def _setup_topology(self):
        ord_keys = self._clusters_keys_olst
        ord_keys_len = len(ord_keys)
        dest_key_index = 1
        for src_key in ord_keys:
            if dest_key_index == ord_keys_len:
                break
            dest_key = ord_keys[dest_key_index]
            source_cluster_name = self._cluster_names_dic[src_key]
            sink_cluster_name = self._cluster_names_dic[dest_key]
            self._join_clusters(source_cluster_name, self._source_master, sink_cluster_name, self._sink_master)
            dest_key_index += 1

    def _join_clusters(self, source_cluster_name, source_master, sink_cluster_name, sink_master):
        self._link_clusters(source_cluster_name, source_master, sink_cluster_name, sink_master)
        time.sleep(self._timeout / 3)
        self._replicate_clusters(source_master, sink_cluster_name)
        if "bidirection" in self._rdirection:
            self._link_clusters(sink_cluster_name, sink_master, source_cluster_name, source_master)
            time.sleep(self._timeout / 3)
            self._replicate_clusters(sink_master, source_cluster_name)

    def _link_clusters(self, source_cluster_name, source_master, sink_cluster_name, sink_master):
        rest_conn_src = RestConnection(source_master)
        rest_conn_src.add_remote_cluster(sink_master.ip, sink_master.port,
            sink_master.rest_username,
            sink_master.rest_password, sink_cluster_name)

    def _replicate_clusters(self, source_master, sink_cluster_name):
        rest_conn_src = RestConnection(source_master)
        for bucket in self._get_cluster_buckets(source_master):
            (rep_database, rep_id) = rest_conn_src.start_replication("continuous", bucket.name, sink_cluster_name)
            time.sleep(5)
        if self._get_cluster_buckets(source_master):
            self._cluster_state_arr.append((rest_conn_src, sink_cluster_name, rep_database, rep_id))

    def _async_load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True, batch_size=1, pause_secs=1, timeout_secs=30):
        tasks = []
        buckets = self._get_cluster_buckets(server)
        for bucket in buckets:
            gen = copy.deepcopy(kv_gen)
            tasks.append(self._cluster_helper.async_load_gen_docs(server, bucket.name, gen,
                                                          bucket.kvs[kv_store],
                                                          op_type, exp, flag, only_store_hash, batch_size, pause_secs, timeout_secs))
        return tasks

    def _load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1, flag=0, only_store_hash=True, batch_size=1000, pause_secs=1, timeout_secs=30):
        tasks = self._async_load_all_buckets(server, kv_gen, op_type, exp, kv_store, flag, only_store_hash, batch_size, pause_secs, timeout_secs)
        for task in tasks:
            task.result()

    def _verify_revIds(self, src_server, snk_server, ops_perf, kv_store=1):
        error_count = 0;
        tasks = []
        #buckets = self._get_cluster_buckets(src_server)
        rest = RestConnection(src_server)
        buckets = rest.get_buckets()
        for bucket in buckets:
            task_info = self._cluster_helper.async_verify_revid(src_server, snk_server, bucket, bucket.kvs[kv_store],
                ops_perf)
            error_count += task_info.err_count
            tasks.append(task_info)
        for task in tasks:
            task.result()

        return error_count

    def _expiry_pager(self, master):
        buckets = self._get_cluster_buckets(master)
        for bucket in buckets:
            ClusterOperationHelper.flushctl_set(master, "exp_pager_stime", 10, bucket)
            self._log.info("wait for expiry pager to run on all these nodes")
            self.sleep(30)

    def _wait_for_stats_all_buckets(self, servers, timeout=120):
        def verify():
            try:
                tasks = []
                buckets = self._get_cluster_buckets(servers[0])
                for server in servers:
                    for bucket in buckets:
                        tasks.append(self._cluster_helper.async_wait_for_stats([server], bucket, '',
                            'ep_queue_size', '==', 0))
                        tasks.append(self._cluster_helper.async_wait_for_stats([server], bucket, '',
                            'ep_flusher_todo', '==', 0))
                for task in tasks:
                    task.result(timeout)
                return True
            except MemcachedError as e:
                self._log.info("verifying ...")
                self._log.debug("Not able to fetch data. Error is %s", (e.message))
                return False

        is_verified = self._poll_for_condition(verify)
        if not is_verified:
            raise ValueError(
                "Verification process not completed after waiting for {0} seconds.".format(self._poll_timeout))

    def _verify_all_buckets(self, server, kv_store=1, timeout=None, max_verify=None, only_store_hash=True, batch_size=1000):
        def verify():
            try:
                tasks = []
                buckets = self._get_cluster_buckets(server)
                for bucket in buckets:
                    tasks.append(self._cluster_helper.async_verify_data(server, bucket, bucket.kvs[kv_store], max_verify, only_store_hash, batch_size))
                for task in tasks:
                    task.result(timeout)
                return True
            except  MemcachedError as e:
                self._log.info("verifying ...")
                self._log.info("Not able to fetch data. Error is %s", (e.message))
                return False

        is_verified = self._poll_for_condition(verify)
        if not is_verified:
            raise ValueError(
                "Verification process not completed after waiting for {0} seconds. Please check logs".format(
                    self._poll_timeout))


    def _verify_stats_all_buckets(self, servers, timeout=120):
        def verify():
            try:
                stats_tasks = []
                buckets = self._get_cluster_buckets(servers[0])
                for bucket in buckets:
                    items = sum([len(kv_store) for kv_store in bucket.kvs.values()])
                    stats_tasks.append(self._cluster_helper.async_wait_for_stats(servers, bucket, '',
                        'curr_items', '==', items))
                    stats_tasks.append(self._cluster_helper.async_wait_for_stats(servers, bucket, '',
                        'vb_active_curr_items', '==', items))

                for task in stats_tasks:
                    task.result(timeout)
                return True
            except  MemcachedError as e:
                self._log.info("verifying ...")
                self._log.debug("Not able to fetch data. Error is %s", (e.message))
                return False

        is_verified = self._poll_for_condition(verify)
        if not is_verified:
            raise ValueError(
                "Verification process not completed after waiting for {0} seconds.".format(self._poll_timeout))

    def _autofail_enable(self, _rest_):
        status = _rest_.update_autofailover_settings(True, self._timeout / 2)
        if not status:
            self._log.info('failed to change autofailover_settings!')
            return
        #read settings and verify
        settings = _rest_.get_autofailover_settings()
        self.assertEquals(settings.enabled, True)

    def _autofail_disable(self, _rest_):
        status = _rest_.update_autofailover_settings(False, self._timeout / 2)
        if not status:
            self._log.info('failed to change autofailover_settings!')
            return
        #read settings and verify
        settings = _rest_.get_autofailover_settings()
        self.assertEquals(settings.enabled, False)

    def wait_for_failover_or_assert(self, master, autofailover_count, timeout):
        time_start = time.time()
        time_max_end = time_start + timeout + 60
        failover_count = 0
        while time.time() < time_max_end:
            failover_count = self.get_failover_count(master)
            if failover_count == autofailover_count:
                break
            time.sleep(2)

        if failover_count != autofailover_count:
            rest = RestConnection(master)
            self._log.info("Latest logs from UI:")
            for i in rest.get_logs(): self._log.error(i)
            self._log.warn("pools/default from {0} : {1}".format(master.ip, rest.cluster_status()))
            self.fail("{0} nodes failed over, expected {1} in {2} seconds".
                            format(failover_count, autofailover_count, time.time() - time_start))
        else:
            self._log.info("{O} nodes failed over as expected")

    def get_failover_count(self, master):
        rest = RestConnection(master)
        cluster_status = rest.cluster_status()

        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            sef._log.info("'clusterMembership' for node {0} is {1}".format(node["otpNode"], node['clusterMembership']))
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1

        return failover_count

    def cbr_routine(self, _healthy_, _compromised_):
        shell = RemoteMachineShellConnection(_healthy_)
        info = shell.extract_remote_info()
        for bucket in self._buckets:
            if info.type.lower() == "linux":
                o, r = shell.execute_command("/opt/couchbase/bin/cbrecovery http://{0}:{1} http://{2}:{3}".format(
                                                    _healthy_.ip, _healthy_.port, _compromised_.ip, _compromised_.port))
                shell.log_command_output(o, r)
            elif info.type.lower() == "windows":
                o, r = shell.execute_command("C:/Program\ Files/Couchbase/Server/bin/cbrecovery.exe http://{0}:{1} http://{2}:{3}".format(
                                                    _healthy_.ip, _healthy_.port, _compromised_.ip, _compromised_.port))
                shell.log_command_output(o, r)
        shell.disconnect()

    def vbucket_map_checker(self, _before_, _after_):
        change_count = 0
        if len(_before)==len(_after_):
            for i in range(len(_before_)):
                if _before_[i][0] != _after_[i][0]:
                    change_count += 1
        return change_count

class cbrTests(preSet):
    def setUp(self):
        super(cbrTests, self).setUp()

    def tearDown(self):
        super(cbrTests, self).tearDown()

    def multiple_failover_swapout_reb_routine(self):
        self._load_all_buckets(self._source_master, self.gen_create, "create", 0)
        tasks = []
        if self._doc_ops is not None:
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self._source_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self._source_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

        vbucket_map_before = []
        vbucket_map_after = []
        time.sleep(self._timeout)
        if self._failover is not None:
            if "source" in self._failover:
                rest = RestConnection(self._source_master)
                vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self._source):
                    self._log.info("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._spares) < self._add_count:
                    self._log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return
                self._log.info("Failing over {0} nodes on source ..".format(self._failover_count))
                failed_nodes = self._source[(len(self._source)-self._failover_count):len(self._source)]
                self._cluster_helper.failover(self._source, failed_nodes)
                for node in failed_nodes:
                    self._source.remove(node)
                time.sleep(self._timeout / 4)
                add_nodes = self._spares[0:self._add_count]

                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self._source.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self._sink_master, self._source_master)

                rest.rebalance(otpNodes=[node.id for node in self._source], ejectedNodes=[failed_nodes])
                rest.rebalance_reached()
                vbucket_map_after = rest.fetch_vbucket_map()

            elif "destination" in self._failover:
                rest = RestConnection(self._sink_master)
                vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self._sink):
                    self._log.info("Won't failover .. count exceeds available servers on sink : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._spares) < self._add_count:
                    self._log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return
                self._log.info("Failing over {0} nodes on destination ..".format(self._failover_count))
                failed_nodes = self._sink[(len(self._sink)-self._failover_count):len(self._sink)]
                self._cluster_helper.failover(self._sink, failed_nodes)
                for node in failed_nodes:
                    self._sink.remove(node)
                time.sleep(self._timeout / 4)
                add_nodes = self._spares[0:self._add_count]

                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self._sink.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self._source_master, self._sink_master)

                rest.rebalance(optNodes=[node.id for node in self._source], ejectedNodes=[failed_nodes])
                rest.rebalance_reached()
                vbucket_map_after = rest.fetch_vbucket_map()

            #TOVERIFY: Ensure vbucket map unchanged if swap rebalance
            if self._failover_count == self._add_count:
                _diff_count_ = self.vbucket_map_checker(vbucket_map_before, vbucket_map_after)
                if _diff_count_ > 0:
                    self.fail("vbucket_map seems to have changed, inspite of swap rebalance!")
                else:
                    self._log.info("vbucket_map retained after swap rebalance")

        time.sleep(self._timeout / 2)
        self.merge_buckets(self._source_master, self._sink_master, bidirection=False)
        self.verify_results()

    def multiple_autofailover_swapout_reb_routine(self):
        failover_reason = self._input.param("failover_reason", "stop_server")     # or firewall_block
        self._load_all_buckets(self._source_master, self.gen_create, "create", 0)
        tasks = []
        if self._doc_ops is not None:
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self._source_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self._source_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

        time.sleep(self._timeout / 2)
        if self._failover is not None:
            if "source" in self._failover:
                rest = RestConnection(self._source_master)
                vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self._source):
                    self._log.info("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._spares) < self._add_count:
                    self._log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return

                self._autofail_enable(rest)
                self._log.info("Triggering {0} over {1} nodes on source ..".format(failover_reason, self._failover_count))
                failed_nodes = self._source[(len(self._source)-self._failover_count):len(self._source)]
                if "stop_server" in failover_reason:
                    for node in failed_nodes:
                        shell = RemoteMachineShellConnection(node)
                        shell.stop_couchbase()
                        shell.disconnect()
                elif "firewall_block" in failover_reason:
                    for node in failed_nodes:
                        shell = RemoteMachineShellConnection(node)
                        shell.log_command_output(o, r)
                        o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:60000 -j REJECT")
                        shell.disconnect()
                self.wait_for_failover_or_assert(self._source_master, self._failover_count, self._timeout)
                for node in failed_nodes:
                    self._source.remove(node)
                time.sleep(self._timeout / 4)
                add_nodes = self._spares[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self._source.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self._sink_master, self._source_master)

                rest.rebalance(otpNodes=[node.id for node in self._source], ejectedNodes=[failed_nodes])
                rest.rebalance_reached()
                vbucket_map_after = rest.fetch_vbucket_map()

                self._autofail_disable(rest)
                if "stop_server" in failover_reason:
                    for node in failed_nodes:
                        shell = RemoteMachineShellConnection(node)
                        shell.start_couchbase()
                        shell.disconnect()
                elif "firewall_block" in failover_reason:
                    for node in failed_nodes:
                        shell = RemoteMachineShellConnection(node)
                        o, r = shell.execute_command("iptables -F")
                        shell.log_command_output(o, r)
                        o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:60000 -j ACCEPT")
                        shell.log_command_output(o, r)
                        shell.disconnect()

            elif "destination" in self._failover:
                rest = RestConnection(self._sink_master)
                vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self._sink):
                    self._log.info("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._spares) < self._add_count:
                    self._log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return

                self._autofail_enable(rest)
                self._log.info("Triggering {0} over {1} nodes on source ..".format(failover_reason, self._failover_count))
                failed_nodes = self._sink[(len(self._sink)-self._failover_count):len(self._sink)]
                if "stop_server" in failover_reason:
                    for node in failed_nodes:
                        shell = RemoteMachineShellConnection(node)
                        shell.stop_couchbase()
                        shell.disconnect()
                elif "firewall_block" in failover_reason:
                    for node in failed_nodes:
                        shell = RemoteMachineShellConnection(node)
                        shell.log_command_output(o, r)
                        o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:60000 -j REJECT")
                        shell.disconnect()
                self.wait_for_failover_or_assert(self._sink_master, self._failover_count, self._timeout)
                for node in failed_nodes:
                    self._sink.remove(node)
                time.sleep(self._timeout / 4)
                add_nodes = self._spares[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self._sink.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self._source_master, self._sink_master)

                rest.rebalance(otpNodes=[node.id for node in self._sink], ejectedNodes=[failed_nodes])
                rest.rebalance_reached()
                vbucket_map_after = rest.fetch_vbucket_map()

                self._autofail_disable(rest)
                if "stop_server" in failover_reason:
                    for node in failed_nodes:
                        shell = RemoteMachineShellConnection(node)
                        shell.start_couchbase()
                        shell.disconnect()
                elif "firewall_block" in failover_reason:
                    for node in failed_nodes:
                        shell = RemoteMachineShellConnection(node)
                        o, r = shell.execute_command("iptables -F")
                        shell.log_command_output(o, r)
                        o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:60000 -j ACCEPT")
                        shell.log_command_output(o, r)
                        shell.disconnect()

            #TOVERIFY: Ensure vbucket map unchanged if swap rebalance
            if self._failover_count == self._add_count:
                _diff_count_ = self.vbucket_map_checker(vbucket_map_before, vbucket_map_after)
                if _diff_count_ > 0:
                    self.fail("vbucket_map seems to have changed, inspite of swap rebalance!")
                else:
                    self._log.info("vbucket_map retained after swap rebalance")

        time.sleep(self._timeout / 2)
        self.merge_buckets(self._source_master, self._sink_master, bidirection=False)
        self.verify_results()    
