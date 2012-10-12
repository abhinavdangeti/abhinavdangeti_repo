import sys
import time
import datetime
import unittest
import logger
import copy

from membase.api.rest_client import RestConnection, Bucket
from couchbase.cluster import Cluster
from couchbase.document import View
from TestInput import TestInputSingleton
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from mc_bin_client import MemcachedError
from membase.api.rest_client import RestConnection

from couchbase.documentgenerator import BlobGenerator
from basetestcase import BaseTestCase

#===============================================================================
# class: XDCRConstants
# Constants used in XDCR application testing. It includes input parameters as well
# as internal constants used in the code.
# All string literals used in the XDCR code shall be defined in this class.
#===============================================================================

class XDCRConstants:
    INPUT_PARAM_REPLICATION_DIRECTION = "rdirection"
    INPUT_PARAM_CLUSTER_TOPOLOGY = "ctopology"
    INPUT_PARAM_SEED_DATA = "sdata"
    INPUT_PARAM_SEED_DATA_MODE = "sdata_mode"

    INPUT_PARAM_SEED_DATA_OPERATION = "sdata_op"

    INPUT_PARAM_POLL_INTERVAL = "poll_interval" # in seconds
    INPUT_PARAM_POLL_TIMEOUT = "poll_timeout"# in seconds

    #CLUSTER_TOPOLOGY_TYPE_LINE = "line"
    CLUSTER_TOPOLOGY_TYPE_CHAIN = "chain"
    CLUSTER_TOPOLOGY_TYPE_STAR = "star"

    REPLICATION_TYPE_CONTINUOUS = "continuous"
    REPLICATION_DIRECTION_UNIDIRECTION = "unidirection"
    REPLICATION_DIRECTION_BIDIRECTION = "bidirection"

    SEED_DATA_MODE_SYNC = "sync"


#===============================================================================
# class: XDCRBaseTest
# This class is the base class (super class) for all the test classes written
# for XDCR. It configures the cluster as defined in the <cluster-file>.ini
# passed as an command line argument "-i" while running the test using testrunner.
# The child classes can override the following methods if required.
# - init_parameters_extended
# - setup_extendedself._input.param(XDCRConstants.INPUT_PARAM_REPLICATION_DIRECTION,
# - teardown_extended
# The reason for providing extended functions is so that inheriting classes can take advantage of
# clean_broken _setup in case an exception comes during test setUp.
# Note:B
# It assumes that the each node is physically up and running.
# In ini file , the cluster name should start with "cluster" (prefix).
#===============================================================================

class XDCRBaseTest(unittest.TestCase):
    def setUp(self):
        try:
            self._log = logger.Logger.get_logger()
            self._input = TestInputSingleton.input
            self._init_parameters()
            self._cluster_helper = Cluster()
            self._log.info("==============  XDCRbasetests setup was started for test #{0} {1}=============="\
                .format(self._case_number, self._testMethodName))
            if not self._input.param("skip_cleanup", False):
                self._cleanup_previous_setup()

            self._init_clusters(self._disabled_consistent_view)
            self.setup_extended()
            self._log.info("==============  XDCRbasetests setup was finished for test #{0} {1} =============="\
                .format(self._case_number, self._testMethodName))
            self._log_start(self)
        except  Exception as e:
            self._log.error(e.message)
            self._log.error("Error while setting up clusters: %s", sys.exc_info())
            self._cleanup_broken_setup()
            raise

    def tearDown(self):
        try:
            self._log.info("==============  XDCRbasetests cleanup was started for test #{0} {1} =============="\
                .format(self._case_number, self._testMethodName))
            self.teardown_extended()
            self._do_cleanup()
            self._log.info("==============  XDCRbasetests cleanup was finished for test #{0} {1} =============="\
                .format(self._case_number, self._testMethodName))
        finally:
            self._cluster_helper.shutdown()
            self._log_finish(self)

    def _cleanup_previous_setup(self):
        self.teardown_extended()
        self._do_cleanup()

    def _init_parameters(self):
        self._log.info("Initializing input parameters started...")
        self._clusters_dic = self._input.clusters # clusters is declared as dic in TestInput which is unordered.
        self._clusters_keys_olst = range(
            len(self._clusters_dic)) #clusters are populated in the dic in testrunner such that ordinal is the key.
        #orderedDic cannot be used in order to maintain the compability with python 2.6
        self._cluster_counter_temp_int = 0
        self._cluster_names_dic = self._get_cluster_names()
        self._servers = self._input.servers
        self._disabled_consistent_view = self._input.param("disabled_consistent_view", True)
        self._floating_servers_set = self._get_floating_servers() # These are the servers defined in .ini file but not linked to any cluster.
        self._cluster_counter_temp_int = 0 #TODO: fix the testrunner code to pass cluster name in params.
        self._buckets = []

        self._default_bucket = self._input.param("default_bucket", True)

        """
        ENTER: sasl_buckets=[no.] or standard_buckets=[no.]
        """
        self._standard_buckets = self._input.param("standard_buckets", 0)
        self._sasl_buckets = self._input.param("sasl_buckets", 0)

        if self._default_bucket:
            self.default_bucket_name = "default"

        self._num_replicas = self._input.param("replicas", 1)
        self._num_items = self._input.param("items", 1000)
        self._value_size = self._input.param("value_size", 256)
        self._dgm_run_bool = self._input.param("dgm_run", False)
        self._mem_quota_int = 0 # will be set in subsequent methods

        self._poll_interval = self._input.param(XDCRConstants.INPUT_PARAM_POLL_INTERVAL, 5)
        self._poll_timeout = self._input.param(XDCRConstants.INPUT_PARAM_POLL_TIMEOUT, 120)

        self.init_parameters_extended()

        self._doc_ops = self._input.param("doc-ops", None)
        if self._doc_ops is not None:
            self._doc_ops = self._doc_ops.split("-")
        self._doc_ops_dest = self._input.param("doc-ops-dest", None)
        # semi-colon separator is not accepted for some reason here
        if self._doc_ops_dest is not None:
            self._doc_ops_dest = self._doc_ops_dest.split("-")

        self._case_number = self._input.param("case_number", 0)
        self._expires = self._input.param("expires", 0)
        self._timeout = self._input.param("timeout", 60)
        self._percent_update = self._input.param("upd", 30)
        self._percent_delete = self._input.param("del", 30)
        self._warmup = self._input.param("warm", None)
        self._failover = self._input.param("failover", None)
        self._rebalance = self._input.param("rebalance", None)
        if self._warmup is not None:
            self._warmup = self._warmup.split("-")
        if self._failover is not None:
            self._failover = self._failover.split("-")
        if self._rebalance is not None:
            self._rebalance = self._rebalance.split("-")
            self._num_rebalance = self._input.param("num_rebalance", 1)


        """
        CREATE's a set of items,
        UPDATE's UPD% of the items starting from 0,
        DELETE's DEL% of the items starting from the end (count(items)).
        """
        self.gen_create = BlobGenerator('loadOne', 'loadOne', self._value_size, end=self._num_items)
        self.gen_delete = BlobGenerator('loadOne', 'loadOne-', self._value_size,
            start=int((self._num_items) * (float)(100 - self._percent_delete) / 100), end=self._num_items)
        self.gen_update = BlobGenerator('loadOne', 'loadOne-', self._value_size, start=0,
            end=int(self._num_items * (float)(self._percent_update) / 100))


        self.ord_keys = self._clusters_keys_olst
        self.ord_keys_len = len(self.ord_keys)

        self.src_nodes = self._clusters_dic[0]
        self.src_master = self.src_nodes[0]

        self.dest_nodes = self._clusters_dic[1]
        self.dest_master = self.dest_nodes[0]

        self._defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self._default_view_name = "default_view"
        self._default_view = View(self._default_view_name, self._defaul_map_func, None)
        self._num_views = self._input.param("num_views", 5)
        self._is_dev_ddoc = self._input.param("is_dev_ddoc", True)

        self.fragmentation_value = self._input.param("fragmentation_value", 80)
        self.disable_src_comp = self._input.param("disable_src_comp", True)
        self.disable_dest_comp = self._input.param("disable_dest_comp", True)

        self._log.info("Initializing input parameters completed.")

    @staticmethod
    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.src_master[0]).log_client_error(msg)
            RestConnection(self.dest_master[0]).log_client_error(msg)
        except:
            pass

    @staticmethod
    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.src_master[0]).log_client_error(msg)
            RestConnection(self.dest_master[0]).log_client_error(msg)
        except:
            pass

    def _get_floating_servers(self):
        cluster_nodes = []
        floating_servers = self._servers

        for key, node in self._clusters_dic.items():
            cluster_nodes.extend(node)

        for c_node in cluster_nodes:
            for node in floating_servers:
                if node.ip in str(c_node) and node.port in str(c_node):
                    floating_servers.remove(node)

        return floating_servers


    def _init_clusters(self, disabled_consistent_view=None):
        for key in self._clusters_keys_olst:
            self._setup_cluster(self._clusters_dic[key], disabled_consistent_view)

    # This method shall be overridden in case there are parameters that need to be initialized.
    def init_parameters_extended(self):
        pass

    # This method shall be overridden in case there are custom steps involved during setup.
    def setup_extended(self):
        pass

    # This method shall be overridden in case there are custom steps involved during teardown.
    def teardown_extended(self):
        pass

    def _do_cleanup(self):
        for key in self._clusters_keys_olst:
            nodes = self._clusters_dic[key]
            BucketOperationHelper.delete_all_buckets_or_assert(nodes, self)
            ClusterOperationHelper.cleanup_cluster(nodes)
            ClusterOperationHelper.wait_for_ns_servers_or_assert(nodes, self)

    def _cleanup_broken_setup(self):
        try:
            self.tearDown()
        except:
            self._log.info("Error while cleaning broken setup.")

    def _get_cluster_names(self):
        cs_names = {}
        for key in self._clusters_keys_olst:
            cs_names[key] = "cluster{0}".format(self._cluster_counter_temp_int)
            self._cluster_counter_temp_int += 1
        return cs_names

    def _setup_cluster(self, nodes, disabled_consistent_view=None):
        self._init_nodes(nodes, disabled_consistent_view)
        self._config_cluster(nodes)
        self._create_buckets(nodes)

    def _init_nodes(self, nodes, disabled_consistent_view=None):
        _tasks = []
        for node in nodes:
            _tasks.append(self._cluster_helper.async_init_node(node, disabled_consistent_view))
        for task in _tasks:
            mem_quota_node = task.result()
            if mem_quota_node < self._mem_quota_int or self._mem_quota_int == 0:
                self._mem_quota_int = mem_quota_node

    def _create_sasl_buckets(self, server, server_id, bucket_size):
        bucket_tasks = []
        for i in range(self._sasl_buckets):
            name = "sasl_bucket_" + str(i + 1)
            bucket_tasks.append(self._cluster_helper.async_create_sasl_bucket(server, name, 'password',
                bucket_size, self._num_replicas))
            self._buckets.append(Bucket(name=name, authType="sasl", saslPassword="password",
                num_replicas=self._num_replicas, bucket_size=bucket_size, master_id=server_id))

        for task in bucket_tasks:
            task.result()

    def _create_standard_buckets(self, server, server_id, bucket_size):
        bucket_tasks = []
        for i in range(self._standard_buckets):
            name = "standard_bucket_" + str(i + 1)
            bucket_tasks.append(self._cluster_helper.async_create_standard_bucket(server, name,
                11214 + i, bucket_size, self._num_replicas))
            self._buckets.append(Bucket(name=name, authType=None, saslPassword=None,
                num_replicas=self._num_replicas, bucket_size=bucket_size, master_id=server_id))

        for task in bucket_tasks:
            task.result()

    def _create_buckets(self, nodes):
        if self._dgm_run_bool:
            self._mem_quota_int = 256
        master_node = nodes[0]
        bucket_size = self._get_bucket_size(master_node, nodes, self._mem_quota_int, self._default_bucket)
        rest = RestConnection(master_node)
        master_id = rest.get_nodes_self().id

        if self._sasl_buckets > 0:
            self._create_sasl_buckets(master_node, master_id, bucket_size)
        if self._standard_buckets > 0:
            self._create_standard_buckets(master_node, master_id, bucket_size)
        if self._default_bucket:
            self._cluster_helper.create_default_bucket(master_node, bucket_size, self._num_replicas)
            self._buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                num_replicas=self._num_replicas, bucket_size=bucket_size, master_id=master_id))

    def _config_cluster(self, nodes):
        task = self._cluster_helper.async_rebalance(nodes, nodes[1:], [])
        task.result()

    def _get_bucket_size(self, master_node, nodes, mem_quota, num_buckets, ratio=2.0 / 3.0):
        for node in nodes:
            if node.ip == master_node.ip:
                return int(ratio / float(len(nodes)) / float(num_buckets) * float(mem_quota))
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
                time.sleep(sleep)
                return self._poll_for_condition_rec(condition, sleep, (num_itr - 1))

    def do_a_warm_up(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.stop_couchbase()
        time.sleep(5)
        shell.start_couchbase()
        shell.disconnect()

    def adding_back_a_node(self, master, server):
        rest = RestConnection(master)
        nodes = rest.node_statuses()
        for node in nodes:
            if server.ip == node.ip and int(server.port) == int(node.port):
                rest.add_back_node(node.id)

    def _get_cluster_buckets(self, master_server):
        rest = RestConnection(master_server)
        master_id = rest.get_nodes_self().id
        #verify if node_ids were changed for cluster_run
        for bucket in self._buckets:
            if ("127.0.0.1" in bucket.master_id and "127.0.0.1" not in master_id) or \
               ("localhost" in bucket.master_id and "localhost" not in master_id):
                new_ip = master_id[master_id.index("@") + 1:]
                bucket.master_id = bucket.master_id.replace("127.0.0.1", new_ip).\
                replace("localhost", new_ip)
        return [bucket for bucket in self._buckets if bucket.master_id == master_id]


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
                kv_store_first[kvs_num].release_partition(key)
                kv_store_second[kvs_num].release_partition(key)
            # return merged kvs, that we expect to get on both clusters
        return kv_store_first[kvs_num]

    def merge_buckets(self, src_master, dest_master, bidirection=True):
        if self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_CHAIN:
            self.do_merge_buckets(src_master, dest_master, bidirection)
        elif self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_STAR:
            for i in range(1, len(self._clusters_dic)):
                dest_cluster = self._clusters_dic[i]
                self.do_merge_buckets(src_master, dest_cluster[0], bidirection)

    def do_merge_buckets(self, src_master, dest_master, bidirection):
        src_buckets = self._get_cluster_buckets(src_master)
        dest_buckets = self._get_cluster_buckets(dest_master)
        for src_bucket in src_buckets:
            for dest_bucket in dest_buckets:
                if src_bucket.name == dest_bucket.name:
                    if bidirection:
                        src_bucket.kvs[1] = self.merge_keys(src_bucket.kvs, dest_bucket.kvs, kvs_num=1)
                    dest_bucket.kvs[1] = src_bucket.kvs[1]

        """Verify the stats at the destination cluster
        1. Data Validity check - using kvstore-node key-value check
        2. Item count check on source versus destination
        3. For deleted and updated items, check the CAS/SeqNo/Expiry/Flags for same key on source/destination
        * Make sure to call expiry_pager function to flush out temp items(deleted/expired items)"""
    def verify_xdcr_stats(self, src_nodes, dest_nodes, verify_src=False):
        if self._num_items in range(0, 10000):
            timeout = 120
        elif self._num_items in range(10000, 50000):
            timeout = 300
        elif self._num_items in range(50000, 100000):
            timeout = 500
        elif self._num_items >= 100000:
            timeout = 600

        if self._failover is not None or self._rebalance is not None:
            timeout *= 3 / 2

        #for verification src and dest clusters need more time
        if verify_src:
            timeout *= 3 / 2

        end_time = time.time() + timeout
        self._log.info("Verify xdcr replication stats at Destination Cluster : {0}".format(self.dest_nodes[0].ip))
        if verify_src:
            timeout = max(120, end_time - time.time())
            self._wait_for_stats_all_buckets(self.src_nodes, timeout=timeout)
        timeout = max(120, end_time - time.time())
        self._wait_for_stats_all_buckets(self.dest_nodes, timeout=timeout)
        self._expiry_pager(self.src_nodes[0])
        self._expiry_pager(self.dest_nodes[0])
        if verify_src:
            timeout = max(120, end_time - time.time())
            self._verify_stats_all_buckets(self.src_nodes, timeout=timeout)
            timeout = max(120, end_time - time.time())
            self._verify_all_buckets(self.src_master, timeout=timeout)
        timeout = max(120, end_time - time.time())
        self._verify_stats_all_buckets(self.dest_nodes, timeout=timeout)
        timeout = max(120, end_time - time.time())
        self._verify_all_buckets(self.dest_master, timeout=timeout)

        errors_caught = 0
        if self._doc_ops is not None or self._doc_ops_dest is not None:
            if "update" in self._doc_ops or (self._doc_ops_dest is not None and "update" in self._doc_ops_dest):
                errors_caught = self._verify_revIds(self.src_nodes[0], self.dest_nodes[0], "update")

            if "delete" in self._doc_ops or (self._doc_ops_dest is not None and "delete" in self._doc_ops_dest):
                errors_caught = self._verify_revIds(self.src_nodes[0], self.dest_nodes[0], "delete")

        if errors_caught > 0:
            self.fail("Mismatches on Meta Information on xdcr-replicated items!")

    def verify_results(self, verify_src=False):
        # Checking replication at destination clusters
        dest_key_index = 1
        for key in self.ord_keys[1:]:
            if dest_key_index == self.ord_keys_len:
                break
            dest_key = self.ord_keys[dest_key_index]
            self.dest_nodes = self._clusters_dic[dest_key]

            self.verify_xdcr_stats(self.src_nodes, self.dest_nodes, verify_src)
            dest_key_index += 1

    def wait_warmup_completed(self, warmupnodes, bucket_names=["default"]):
        if isinstance(bucket_names, str):
            bucket_names = [bucket_names]
        for server in warmupnodes:
            for bucket in bucket_names:
                mc = MemcachedClientHelper.direct_client(server, bucket)
                start = time.time()
                while time.time() - start < 150:
                    if mc.stats()["ep_warmup_thread"] == "complete":
                        self._log.info("Warmed up: %s items " % (mc.stats()["curr_items_tot"]))
                        time.sleep(10)
                        break
                    elif mc.stats()["ep_warmup_thread"] == "running":
                        self._log.info(
                            "Still warming up .. curr_items_tot : %s" % (mc.stats()["curr_items_tot"]))
                        continue
                    else:
                        self._log.info("Value of ep_warmup_thread does not exist, exiting from this server")
                        break
                if mc.stats()["ep_warmup_thread"] == "running":
                    self._log.info("ERROR: ep_warmup_thread's status not complete")
                mc.close


    def _modify_src_data(self):
        """Setting up creates/updates/deletes at source nodes"""

        if self._doc_ops is not None:
            if "create" in self._doc_ops:
                self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
            if "update" in self._doc_ops:
                self._load_all_buckets(self.src_master, self.gen_update, "update", self._expires)
            if "delete" in self._doc_ops:
                self._load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
            self._wait_for_stats_all_buckets(self.src_nodes)

    def disable_compaction(self, server=None, bucket="default"):
        server = server or self.src_master
        new_config = {"viewFragmntThresholdPercentage" : None,
                      "dbFragmentThresholdPercentage" :  None,
                      "dbFragmentThreshold" : None,
                      "viewFragmntThreshold" : None}
        self._cluster_helper.modify_fragmentation_config(server, new_config, bucket)

    def make_default_views(self, prefix, count, is_dev_ddoc=False,):
        ref_view = self._default_view
        ref_view.name = (prefix, ref_view.name)[prefix is None]
        return [View(ref_view.name + str(i), ref_view.map_func, None, is_dev_ddoc) for i in xrange(count)]


    def async_create_views(self, server, design_doc_name, views, bucket="default"):
        tasks = []
        if len(views):
            for view in views:
                t_ = self._cluster_helper.async_create_view(server, design_doc_name, view, bucket)
                tasks.append(t_)
        else:
            t_ = self._cluster_helper.async_create_view(server, design_doc_name, None, bucket)
            tasks.append(t_)
        return tasks

#===============================================================================
# class: XDCRReplicationBaseTest
# This class links the different clusters defined in ".ini" file and starts the
# replication. It supports different topologies for cluster which can be defined
# by input parameter "ctopology". The various values supported are as follows
# - chain
# - star (Not Implemented yet)
# - tree (Not Implemented yet)
# The direction of replication can be defined using parameter "rdirection".
# It supports following values
# - unidirection
# - bidirection
# Note:
# The first node is consideredn as master node in the respective cluster section
# of .ini file.
# Cluster/Node ordinals are determined by the sequence of entry of node in .ini file.
#===============================================================================

class XDCRReplicationBaseTest(XDCRBaseTest):
    def setup_extended(self):
        self._setup_topology()
        self._load_data()


    def teardown_extended(self):
        for _, clusters in self._clusters_dic.items():
            rest = RestConnection(clusters[0])
            rest.remove_all_remote_clusters()
            rest.remove_all_replications()
#            #TODO should be added 'stop replication' when API to stop will be implemented
#        for (rest_conn, cluster_ref, rep_database, rep_id) in self._cluster_state_arr:
#            rest_conn.stop_replication(rep_database, rep_id)
            #rest_conn.remove_remote_cluster(cluster_ref)


    def init_parameters_extended(self):
        self._cluster_state_arr = []
        self._cluster_topology_str = self._input.param(XDCRConstants.INPUT_PARAM_CLUSTER_TOPOLOGY,
            XDCRConstants.CLUSTER_TOPOLOGY_TYPE_CHAIN)
        self._replication_direction_str = self._input.param(XDCRConstants.INPUT_PARAM_REPLICATION_DIRECTION,
            XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION)
        self._seed_data = self._input.param(XDCRConstants.INPUT_PARAM_SEED_DATA, False)

        self._seed_data_ops_lst = self._input.param(XDCRConstants.INPUT_PARAM_SEED_DATA_OPERATION,
            "create").split('|')
        self._seed_data_mode_str = self._input.param(XDCRConstants.INPUT_PARAM_SEED_DATA_MODE,
            XDCRConstants.SEED_DATA_MODE_SYNC)


    def _setup_topology(self):
        if self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_CHAIN:
            self._setup_topology_chain()
        elif self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_STAR:
            self._set_toplogy_star()
        else:
            raise "Not supported cluster topology : %s", self._cluster_topolog_str


    def _load_data(self):
        if not self._seed_data:
            return
        if self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_CHAIN:
            self._load_data_chain()
        elif self._cluster_topology_str == XDCRConstants.CLUSTER_TOPOLOGY_TYPE_STAR:
            self._load_data_star()
        else:
            raise "Seed data not supported cluster topology : %s", self._cluster_topology_str


    def _setup_topology_chain(self):
        ord_keys = self._clusters_keys_olst
        ord_keys_len = len(ord_keys)
        dest_key_index = 1
        for src_key in ord_keys:
            if dest_key_index == ord_keys_len:
                break
            dest_key = ord_keys[dest_key_index]
            src_cluster_name = self._cluster_names_dic[src_key]
            dest_cluster_name = self._cluster_names_dic[dest_key]
            self._join_clusters(src_cluster_name, self.src_master, dest_cluster_name, self.dest_master)
            dest_key_index += 1


    def _set_toplogy_star(self):
        src_master_identified = False
        for key in self._clusters_keys_olst:
            nodes = self._clusters_dic[key]
            if not src_master_identified:
                src_cluster_name = self._cluster_names_dic[key]
                self.src_master = nodes[0]
                src_master_identified = True
                continue
            dest_cluster_name = self._cluster_names_dic[key]
            self.dest_master = nodes[0]
            self._join_clusters(src_cluster_name, self.src_master, dest_cluster_name, self.dest_master)
            time.sleep(30)


    def _load_data_chain(self):
        for key in self._clusters_keys_olst:
            cluster_node = self._clusters_dic[key][0]
            cluster_name = self._cluster_names_dic[key]
            self._log.info("Starting Load # items {0} node {1} , cluster {2}....".format(self._num_items, cluster_node,
                cluster_name))
            self._load_gen_data(cluster_name, cluster_node)
            if self._replication_direction_str == XDCRConstants.REPLICATION_DIRECTION_UNIDIRECTION:
                break
            self._log.info("Completed Load")


    def set_environ_param(self, interval_time):
        for server in self.src_nodes:
            shell = RemoteMachineShellConnection(server)
            shell.set_environment_variable('XDCR_FAILURE_RESTART_INTERVAL', interval_time)
        if self._replication_direction_str == XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION:
            for server in self.dest_nodes:
                shell = RemoteMachineShellConnection(server)
                shell.set_environment_variable('XDCR_FAILURE_RESTART_INTERVAL', interval_time)


    def _join_clusters(self, src_cluster_name, src_master, dest_cluster_name, dest_master):
        if src_master.ip != dest_master.ip:
            if self._failover is not None or self._warmup is not None:
                self.set_environ_param(1)
        time.sleep(self._timeout / 2)
        self._link_clusters(src_cluster_name, src_master, dest_cluster_name, dest_master)
        self._replicate_clusters(src_master, dest_cluster_name)
        if self._replication_direction_str == XDCRConstants.REPLICATION_DIRECTION_BIDIRECTION:
            self._link_clusters(dest_cluster_name, dest_master, src_cluster_name, src_master)
            self._replicate_clusters(dest_master, src_cluster_name)


    def _link_clusters(self, src_cluster_name, src_master, dest_cluster_name, dest_master):
        rest_conn_src = RestConnection(src_master)
        rest_conn_src.add_remote_cluster(dest_master.ip, dest_master.port,
            dest_master.rest_username,
            dest_master.rest_password, dest_cluster_name)


    def _replicate_clusters(self, src_master, dest_cluster_name):
        rest_conn_src = RestConnection(src_master)
        for bucket in self._get_cluster_buckets(src_master):
            (rep_database, rep_id) = rest_conn_src.start_replication(XDCRConstants.REPLICATION_TYPE_CONTINUOUS,
                bucket.name, dest_cluster_name)
            time.sleep(5)
        if self._get_cluster_buckets(src_master):
            self._cluster_state_arr.append((rest_conn_src, dest_cluster_name, rep_database, rep_id))


    def _load_gen_data(self, cname, node):
        for op_type in self._seed_data_ops_lst:
            num_items_ratio = self._get_num_items_ratio(op_type)
            load_gen = BlobGenerator(cname, cname, self._value_size, end=num_items_ratio)
            self._log.info("Starting Load operation '{0}' for items (ratio) '{1}' on node '{2}'....".format(op_type,
                num_items_ratio, cname))
            if self._seed_data_mode_str == XDCRConstants.SEED_DATA_MODE_SYNC:
                self._load_all_buckets(node, load_gen, op_type, 0)
                self._log.info("Completed Load of {0}".format(op_type))
            else:
                self._async_load_all_buckets(node, load_gen, op_type, 0)
                self._log.info("Started async Load of {0}".format(op_type))

    def _get_num_items_ratio(self, op_type):
        if op_type in ["update", "delete"]:
            return self._num_items / 3
        else:
            return self._num_items

    def _async_load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1):
        tasks = []
        buckets = self._get_cluster_buckets(server)
        for bucket in buckets:
            gen = copy.deepcopy(kv_gen)
            tasks.append(self._cluster_helper.async_load_gen_docs(server, bucket.name, gen,
                bucket.kvs[kv_store],
                op_type, exp))
        return tasks


    def _load_all_buckets(self, server, kv_gen, op_type, exp, kv_store=1):
        tasks = self._async_load_all_buckets(server, kv_gen, op_type, exp, kv_store)
        for task in tasks:
            task.result()

    def _verify_revIds(self, src_server, dest_server, ops_perf, kv_store=1):
        error_count = 0;
        tasks = []
        #buckets = self._get_cluster_buckets(src_server)
        rest = RestConnection(src_server)
        buckets = rest.get_buckets()
        for bucket in buckets:
            task_info = self._cluster_helper.async_verify_revid(src_server, dest_server, bucket, bucket.kvs[kv_store],
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
            time.sleep(30)

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

    def _verify_all_buckets(self, server, kv_store=1, timeout=120):
        def verify():
            try:
                tasks = []
                buckets = self._get_cluster_buckets(server)
                for bucket in buckets:
                    tasks.append(self._cluster_helper.async_verify_data(server, bucket, bucket.kvs[kv_store]))
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

#                    available_replicas = self._num_replicas
#                    if len(servers) == self._num_replicas:
#                        available_replicas = len(servers) - 1
#                    elif len(servers) <= self._num_replicas:
#                        available_replicas = len(servers) - 1
#
#                    stats_tasks.append(self._cluster_helper.async_wait_for_stats(servers, bucket, '',
#                        'vb_replica_curr_items', '==', items * available_replicas))
#                    stats_tasks.append(self._cluster_helper.async_wait_for_stats(servers, bucket, '',
#                        'curr_items_tot', '==', items * (available_replicas + 1)))

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

    def _async_failover(self, src_nodes, failover_node):
        tasks = []
        tasks.append(self._cluster_helper.async_failover(src_nodes, failover_node))
        return tasks

    def _async_rebalance(self, src_nodes, to_add_node, to_remove_node):
        tasks = []
        tasks.append(self._cluster_helper.async_rebalance(src_nodes, to_add_node, to_remove_node))
        return tasks
    def _find_cluster_nodes_by_name(self, cluster_name):
        return self._clusters_dic[[k for k, v in self._cluster_names_dic.iteritems() if v == cluster_name][0]]

    def _find_key_from_cluster_name(self, cluster_name):
        for k, v in self._cluster_names_dic.iteritems():
            if v == cluster_name:
                return k
        return -1

    def _enable_firewall(self, server):
        shell = RemoteMachineShellConnection(server)
        o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:60000 -j REJECT")
        shell.log_command_output(o, r)
        if self._replication_direction_str == "bidirection":
            o, r = shell.execute_command("/sbin/iptables -A OUTPUT -p tcp -o eth0 --dport 1000:60000 -j REJECT")
            shell.log_command_output(o, r)
        o, r = shell.execute_command("/sbin/iptables -A INPUT -m state --state ESTABLISHED, RELATED -j ACCEPT")
        shell.log_command_output(o, r)
        self._log.info("enabled firewall on {0}".format(server))
        o, r = shell.execute_command("/sbin/iptables --list")
        shell.log_command_output(o, r)
        shell.disconnect()

    def _disable_firewall(self, server):
        shell = RemoteMachineShellConnection(server)
        o, r = shell.execute_command("iptables -F")
        shell.log_command_output(o, r)
        o, r = shell.execute_command("/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:60000 -j ACCEPT")
        shell.log_command_output(o, r)
        if self._replication_direction_str == "bidirection":
            o, r = shell.execute_command("/sbin/iptables -A OUTPUT -p tcp -o eth0 --dport 1000:60000 -j ACCEPT")
            shell.log_command_output(o, r)
        o, r = shell.execute_command("/sbin/iptables -A INPUT -m state --state ESTABLISHED, RELATED -j ACCEPT")
        shell.log_command_output(o, r)
        self._log.info("enabled firewall on {0}".format(server))
        o, r = shell.execute_command("/sbin/iptables --list")
        shell.log_command_output(o, r)
        shell.disconnect()
