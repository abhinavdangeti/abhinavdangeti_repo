from xdcr.xdcrbasetests import XDCRReplicationBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from random import randrange

class CBRbaseclass(XDCRReplicationBaseTest):
    def _autofail_enable(self, _rest_):
        status = _rest_.update_autofailover_settings(True, self._timeout / 2)
        if not status:
            self.log.info('failed to change autofailover_settings!')
            return
        #read settings and verify
        settings = _rest_.get_autofailover_settings()
        self.assertEquals(settings.enabled, True)

    def _autofail_disable(self, _rest_):
        status = _rest_.update_autofailover_settings(False, self._timeout / 2)
        if not status:
            self.log.info('failed to change autofailover_settings!')
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
            self.sleep(2)

        if failover_count != autofailover_count:
            rest = RestConnection(master)
            self.log.info("Latest logs from UI:")
            for i in rest.getlogs(): self.log.error(i)
            self.log.warn("pools/default from {0} : {1}".format(master.ip, rest.cluster_status()))
            self.fail("{0} nodes failed over, expected {1} in {2} seconds".
                            format(failover_count, autofailover_count, time.time() - time_start))
        else:
            self.log.info("{O} nodes failed over as expected")

    def get_failover_count(self, master):
        rest = RestConnection(master)
        cluster_status = rest.cluster_status()

        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            sef.log.info("'clusterMembership' for node {0} is {1}".format(node["otpNode"], node['clusterMembership']))
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1

        return failover_count

    def cbr_routine(self, _healthy_, _compromised_):
        shell = RemoteMachineShellConnection(_healthy_)
        info = shell.extract_remote_info()
        for bucket in self.buckets:
            if info.type.lower() == "linux":
                o, r = shell.execute_command_raw("/opt/couchbase/bin/cbrecovery http://{0}:{1}@{2}:{3} http://{4}:{5}@{6}:{7} -b {8} -B {8}".format(
                                                    _healthy_.rest_username, _healthy_.rest_password, _healthy_.ip, _healthy_.port, 
                                                    _compromised_.rest_username, _compromised_.rest_password, _compromised_.ip, _compromised_.port,
                                                    bucket.name))
            elif info.type.lower() == "windows":
                o, r = shell.execute_command("C:/Program\ Files/Couchbase/Server/bin/cbrecovery.exe http://{0}:{1}@{2}:{3} http://{4}:{5}@{6}:{7} -b {8} -B {8}".format(
                                                    _healthy_.rest_username, _healthy_.rest_password, _healthy_.ip, _healthy_.port, 
                                                    _compromised_.rest_username, _compromised_.rest_password, _compromised_.ip, _compromised_.port,
                                                    bucket.name))
#        shell.log_command_output(o, r)
        shell.disconnect()

    def vbucket_map_checker(self, _before_, _after_):
        change_count = 0
        if len(_before)==len(_after_):
            for i in range(len(_before_)):
                if _before_[i][0] != _after_[i][0]:
                    change_count += 1
        return change_count


#Assumption that at least 2 nodes on every cluster
class cbrecovery(CBRbaseclass, XDCRReplicationBaseTest):
    def setUp(self):
        super(cbrecovery, self).setUp()
        self._failover_count = self._input.param("fail_count", 0)
        self._add_count = self._input.param("add_count", 0) 

    def tearDown(self):
        super(cbrecovery, self).tearDown()

    def cbrecover_multiple_failover_swapout_reb_routine(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        tasks = []
        if self._doc_ops is not None:
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

        self.sleep(self._timeout / 3)
        vbucket_map_before = []
        vbucket_map_after = []

        if self._failover is not None:
            if "source" in self._failover:
                rest = RestConnection(self.src_master)
                vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.src_nodes):
                    self.log.info("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._floating_servers_set) < self._add_count:
                    self.log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return
                self.log.info("Failing over {0} nodes on source ..".format(self._failover_count))
                failed_nodes = self.src_nodes[(len(self.src_nodes)-self._failover_count):len(self.src_nodes)]
                self._cluster_helper.failover(self.src_nodes, failed_nodes)
                for node in failed_nodes:
                    self.src_nodes.remove(node)
                self.sleep(self._timeout / 4)
                add_nodes = self._floating_servers_set[0:self._add_count]

                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.src_nodes.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.dest_master, self.src_master)

                rest.rebalance(otpNodes=[node.id for node in self.src_nodes], ejectedNodes=[failed_nodes])
                rest.rebalance_reached()
                vbucket_map_after = rest.fetch_vbucket_map()

            elif "destination" in self._failover:
                rest = RestConnection(self.dest_master)
                vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.dest_nodes):
                    self.log.info("Won't failover .. count exceeds available servers on sink : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._floating_servers_set) < self._add_count:
                    self.log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return
                self.log.info("Failing over {0} nodes on destination ..".format(self._failover_count))
                failed_nodes = self.dest_nodes[(len(self.dest_nodes)-self._failover_count):len(self.dest_nodes)]
                self._cluster_helper.failover(self.dest_nodes, failed_nodes)
                for node in failed_nodes:
                    self.dest_nodes.remove(node)
                self.sleep(self._timeout / 4)
                add_nodes = self._floating_servers_set[0:self._add_count]

                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.dest_nodes.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.src_master, self.dest_master)

                rest.rebalance(optNodes=[node.id for node in self.src_nodes], ejectedNodes=[failed_nodes])
                rest.rebalance_reached()
                vbucket_map_after = rest.fetch_vbucket_map()

            #TOVERIFY: Ensure vbucket map unchanged if swap rebalance
            if self._failover_count == self._add_count:
                _diff_count_ = self.vbucket_map_checker(vbucket_map_before, vbucket_map_after)
                if _diff_count_ > 0:
                    self.fail("vbucket_map seems to have changed, inspite of swap rebalance!")
                else:
                    self.log.info("vbucket_map retained after swap rebalance")

        self.sleep(self._timeout / 2)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results()

    def cbrecover_multiple_autofailover_swapout_reb_routine(self):
        failover_reason = self._input.param("failover_reason", "stop_server")     # or firewall_block
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        tasks = []
        if self._doc_ops is not None:
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

        self.sleep(self._timeout / 2)
        if self._failover is not None:
            if "source" in self._failover:
                rest = RestConnection(self.src_master)
                vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.src_nodes):
                    self.log.info("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._floating_servers_set) < self._add_count:
                    self.log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return

                self._autofail_enable(rest)
                self.log.info("Triggering {0} over {1} nodes on source ..".format(failover_reason, self._failover_count))
                failed_nodes = self.src_nodes[(len(self.src_nodes)-self._failover_count):len(self.src_nodes)]
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
                self.wait_for_failover_or_assert(self.src_master, self._failover_count, self._timeout)
                for node in failed_nodes:
                    self.src_nodes.remove(node)
                self.sleep(self._timeout / 4)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.src_nodes.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.dest_master, self.src_master)

                rest.rebalance(otpNodes=[node.id for node in self.src_nodes], ejectedNodes=[failed_nodes])
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
                rest = RestConnection(self.dest_master)
                vbucket_map_before = rest.fetch_vbucket_map()       # JUST FOR DEFAULT BUCKET AS OF NOW
                if self._failover_count >= len(self.dest_nodes):
                    self.log.info("Won't failover .. count exceeds available servers on source : SKIPPING TEST")
                    self.tearDown()
                    return
                if len(self._floating_servers_set) < self._add_count:
                    self.log.info("Not enough spare nodes available, to match the failover count : SKIPPING TEST")
                    self.tearDown()
                    return

                self._autofail_enable(rest)
                self.log.info("Triggering {0} over {1} nodes on source ..".format(failover_reason, self._failover_count))
                failed_nodes = self.dest_nodes[(len(self.dest_nodes)-self._failover_count):len(self.dest_nodes)]
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
                self.wait_for_failover_or_assert(self.dest_master, self._failover_count, self._timeout)
                for node in failed_nodes:
                    self.dest_nodes.remove(node)
                self.sleep(self._timeout / 4)
                add_nodes = self._floating_servers_set[0:self._add_count]
                for node in add_nodes:
                    rest.add_node(user=node.rest_username, password=node.rest_password, remoteIp=node.ip, port=node.port)
                self.dest_nodes.extend(add_nodes)
                # CALL THE CBRECOVERY ROUTINE
                self.cbr_routine(self.src_master, self.dest_master)

                rest.rebalance(otpNodes=[node.id for node in self.dest_nodes], ejectedNodes=[failed_nodes])
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
                    self.log.info("vbucket_map retained after swap rebalance")

        self.sleep(self._timeout / 2)
        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        self.verify_results()
