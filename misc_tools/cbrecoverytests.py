import sys
import unittest
import logger
import copy

from membase.api.rest_client import RestConnection, Bucket
from couchbase.cluster import Cluster
from couchbase.document import View
from TestInput import TestInputSingleton
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from mc_bin_client import MemcachedError
from basetestcase import BaseTestCase
from xdcr.xdcrbasetests import XDCRReplicationBaseTest

class preSet:
    def setUp(self):
        try:
            self._xdcr_ref = XDCRReplicationBaseTest()
            self._xdcr_ref.setUp()

        except Exception as e:
            self.log.error(e.message)
            self.log.error("Error while setting up the cluster: %s", sys.exc_info())
            self._xdcr_ref._cleanup_broken_setup()

    def tearDown(self):
        try:
            self._xdcr_ref.tearDown()
        finally:
            self._cluster_helper.shutdown()
            self._xdcr_ref._log_finish(self)

    def _init_parameters(self):
        self._log = logger.Logger.get_logger()
        self._rdirection = self._xdcr_ref._input.param("rdirection","unidirection")
        self._servers = self._xdcr_ref._servers
        self._source = self._xdcr_ref.src_nodes
        self._sink = self._xdcr_ref.dest_nodes
        self._spares = self._xdcr_ref._floating_servers_set

        self._case_number = self._xdcr_ref._input.param("case_number", 0)
        self._expires = self._xdcr_ref._input.param("expires", 0)
        self._timeout = self._xdcr_ref._input.param("timeout", 60)
        self._percent_update = self._xdcr_ref._input.param("upd", 30)
        self._percent_delete = self._xdcr_ref._input.param("del", 30)

        self._failover = self._xdcr_ref._input.param("failover", None)
        if self._failover is not None:
            self._failover = self._failover.split("-")
        self._failover_count = self._xdcr_ref._input.param("fail_count", 0)
        self._add_count = self._xdcr_ref._input.param("add_count", 0)

        #SUPPORTING JUST THE DEFAULT BUCKET FOR NOW
        self._buckets = []
        self._default_bucket = self._xdcr_ref._default_bucket
#        self._sasl_buckets = self._xdcr_ref._sasl_buckets
#        self._standard_buckets = self._xdcr_ref._standard_buckets
        self._replicas = self._xdcr_ref._num_replicas

        self._source_master = self._source[0]
        self._sink_master = self._sink[0]

    def _autofail_enable(self, _rest_):
        status = _rest_.update_autofailover_settings(True, self._xdcr_ref._timeout / 2)
        if not status:
            self._log.info('failed to change autofailover_settings!')
            return
        #read settings and verify
        settings = _rest_.get_autofailover_settings()
        self.assertEquals(settings.enabled, True)

    def _autofail_disable(self, _rest_):
        status = _rest_.update_autofailover_settings(False, self._xdcr_ref._timeout / 2)
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
        self._xdcr_ref._load_all_buckets(self._xdcr_ref.src_master, self._xdcr_ref.gen_create, "create", 0)
        tasks = []
        if self._xdcr_ref._doc_ops is not None:
            if "update" in self._xdcr_ref._doc_ops:
                tasks.extend(self._xdcr_ref._async_load_all_buckets(self._xdcr_ref.src_master, self._xdcr_ref.gen_update, "update", self._xdcr_ref._expires)
            if "delete" in self._xdcr_ref._doc_ops:
                tasks.extend(self._xdcr_ref._async_load_all_buckets(self._xdcr_ref.src_master, self._xdcr_ref.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

        vbucket_map_before = []
        vbucket_map_after = []
        time.sleep(self._xdcr_ref._timeout)
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
                self._xdcr_ref._cluster_helper.failover(self._source, failed_nodes)
                for node in failed_nodes:
                    self._source.remove(node)
                time.sleep(self._xdcr_ref._timeout / 4)
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
                self._xdcr_ref._cluster_helper.failover(self._sink, failed_nodes)
                for node in failed_nodes:
                    self._sink.remove(node)
                time.sleep(self._xdcr_ref._timeout / 4)
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

        time.sleep(self._xdcr_ref._timeout / 2)
        self.merge_buckets(self._source_master, self._sink_master, bidirection=False)
        self.verify_results()

    def multiple_autofailover_swapout_reb_routine(self):
        failover_reason = self._xdcr_ref._input.param("failover_reason", "stop_server")     # or firewall_block
        self._xdcr_ref._load_all_buckets(self._xdcr_ref.src_master, self._xdcr_ref.gen_create, "create", 0)
        tasks = []
        if self._xdcr_ref._doc_ops is not None:
            if "update" in self._xdcr_ref._doc_ops:
                tasks.extend(self._xdcr_ref._async_load_all_buckets(self._xdcr_ref.src_master, self._xdcr_ref.gen_update, "update", self._xdcr_ref._expires)
            if "delete" in self._xdcr_ref._doc_ops:
                tasks.extend(self._xdcr_ref._async_load_all_buckets(self._xdcr_ref.src_master, self._xdcr_ref.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

        time.sleep(self._xdcr_ref._timeout / 2)
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
                self.wait_for_failover_or_assert(self._source_master, self._failover_count, self._xdcr_ref._timeout)
                for node in failed_nodes:
                    self._source.remove(node)
                time.sleep(self._xdcr_ref._timeout / 4)
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
                self.wait_for_failover_or_assert(self._sink_master, self._failover_count, self._xdcr_ref._timeout)
                for node in failed_nodes:
                    self._sink.remove(node)
                time.sleep(self._xdcr_ref._timeout / 4)
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

        time.sleep(self._xdcr_ref._timeout / 2)
        self.merge_buckets(self._source_master, self._sink_master, bidirection=False)
        self.verify_results()    
