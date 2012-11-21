from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from xdcrbasetests import XDCRReplicationBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, Bucket
import random
import datetime
import unittest
import time

N = 99999999

#SIMULATES XDCR UNDER VARIOUS OPERATIONS: REBALANCES, FAILOVERS, REBOOTS, RESTARTS, FIREWALL-BLOCKING pseudo-randomly
#START OFF WITH AT LEAST 2 NODES IN EACH CLUSTER AND AT LEAST 2 NODES IN THE FLOATING SERVER SET

class basesim(unittest.TestCase):
    def wait_for_failover_or_assert(master, autofailover_count, timeout):
        time_start = time.time()
        time_max_end = time_start + timeout + 60
        failover_count = 0
        while time.time() < time_max_end:
            failover_count = self.get_failover_count(master)
            if failover_count == autofailover_count:
                break
            time.sleep(2)

        testcase.assertTrue(failover_count == autofailover_count, "{0} nodes failed over, expected {1} in {2} seconds".
                            format(failover_count, autofailover_count, time.time() - time_start))

    def get_failover_count(master):
        rest = RestConnection(master)
        cluster_status = rest.cluster_status()
        log = logger.Logger().get_logger()

        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            log.info("'clusterMembership' for node {0} is {1}".format(node["otpNode"], node['clusterMembership']))
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1

        return failover_count

class sim(XDCRReplicationBaseTest):
    def setUp(self):
        super(sim, self).setUp()
        self.gen_create2 = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self._num_items)
        self._output_ = []
        self._test_count = 0

    def tearDown(self):
        super(sim, self).tearDown()

    def chaos_maker(self):
        self.set_environ_param(1)
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)    
        if self._replication_direction_str in "bidirection":
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)
        time.sleep(self._timeout)
        
        start_time = datetime.datetime.now()
        while True:
            _r = random.randint(0,N)
            toss1 = random.randint(0,1000)
            task_begin = datetime.datetime.now()
            if _r%31==0 or _r%37==0 or _r%13==0 or _r%17==0 or _r%29==0 or _r%7==0 or _r%11==0 or _r%5==0 or _r%19==0 or _r%23==0 or _r%41==0 or _r%43==0 or _r%47==0:
                if toss1 % 2 == 0:
                    self._log.info("UPDATE OPS IN PROGRESS ... ")
                    src_buckets = self._get_cluster_buckets(self.src_master)
                    for bucket in src_buckets:
                        valid_keys, deleted_keys = bucket.kvs[1].key_set()
                        item_count = len(valid_keys) + len(deleted_keys)
                    g_update1 = BlobGenerator('loadOne', 'loadOne-', self._value_size, start=0, 
                                              end=int(item_count * (float)(self._percent_update) / 100))
                    self._load_all_buckets(self.src_master, g_update1, "update", self._expires)    
                    if self._replication_direction_str in "bidirection":
                        dest_buckets = self._get_cluster_buckets(self.dest_master)
                        for bucket in dest_buckets:
                            valid_keys, deleted_keys = bucket.kvs[1].key_set()
                            item_count = len(valid_keys) + len(deleted_keys)
                        g_update2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, start=0, 
                                                  end=int(item_count * (float)(self._percent_update) / 100))
                        self._load_all_buckets(self.dest_master, g_update2, "update", self._expires)
                time.sleep(self._timeout / 2)
            tasks = []

            if _r%31==0:
                task_name = "Rebalance_in_on_source"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                if self._floating_servers_set is None:
                    continue
                node = self._floating_servers_set[0]
                tasks.extend(self._async_rebalance(self.src_nodes, [node], []))
                self.src_nodes.extend([node])
                self._floating_servers_set.remove(node)
                time.sleep(self._timeout / 3)
                self._test_count += 1

            elif _r%37==0:
                task_name = "Rebalance_in_on_destination"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                if self._floating_servers_set is None:
                    continue
                node = self._floating_servers_set[0]
                tasks.extend(self._async_rebalance(self.dest_nodes, [node], []))
                self.dest_nodes.extend([node])
                self._floating_servers_set.remove(node)
                time.sleep(self._timeout / 3)
                self._test_count += 1

            elif _r%13==0:
                task_name = "Failover_on_source"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                rest = RestConnection(self.src_master)
                nodes = rest.node_statuses()
                _num = random.randint(0, len(self.src_nodes)-1)
                chosen = RebalanceHelper.pick_nodes(self.src_master, howmany=_num)
                for node in chosen:
                    failed_over = rest.fail_over(node.id)
                    if not failed_over:
                        self.log.info("unable to failover the node the first time. try again in  60 seconds..")
                        time.sleep(60)
                        failed_over = rest.fail_over(node.id)
                    self.assertTrue(failed_over, "unable to failover node")
                    self._log.info("failed over node : {0}".format(node.id))
                rest.rebalance(otpNodes=[node.id for node in nodes],
                               ejectedNodes=[node.id for node in chosen])
                msg = "rebalance failed while removing failover nodes {0}".format(chosen)
                self.assertTrue(rest.monitorRebalance(stop_if_loop=True), msg=msg)
                for failed in chosen:
                    for server in self.src_nodes:
                        if server.ip == failed.ip:
                            self.src_nodes.remove(server)
                            self._floating_servers_set.extend([server])
                self._test_count += 1

            elif _r%17==0:
                task_name = "Failover_on_destination"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                rest = RestConnection(self.dest_master)
                nodes = rest.node_statuses()
                _num = random.randint(0, len(self.dest_nodes)-1)
                chosen = RebalanceHelper.pick_nodes(self.dest_master, howmany=_num)
                for node in chosen:
                    failed_over = rest.fail_over(node.id)
                    if not failed_over:
                        self.log.info("unable to failover the node the first time. try again in  60 seconds..")
                        time.sleep(60)
                        failed_over = rest.fail_over(node.id)
                    self.assertTrue(failed_over, "unable to failover node")
                    self._log.info("failed over node : {0}".format(node.id))
                rest.rebalance(otpNodes=[node.id for node in nodes],
                               ejectedNodes=[node.id for node in chosen])
                msg = "rebalance failed while removing failover nodes {0}".format(chosen)
                self.assertTrue(rest.monitorRebalance(stop_if_loop=True), msg=msg)
                for failed in chosen:
                    for server in self.dest_nodes:
                        if server.ip == failed.ip:
                            self.dest_nodes.remove(server)
                            self._floating_servers_set.extend([server])
                self._test_count += 1

            elif _r%29==0:
                task_name = "Failover_one_on_source_&_destination"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                if len(self.src_nodes) > 1 and len(self.dest_nodes) > 1:
                    i = len(self.src_nodes) - 1
                    j = len(self.dest_nodes) - 1
                    removed_nodes = []
                    self._log.info(
                            " Failing over Source Non-Master Node {0}:{1}".format(self.src_nodes[i].ip, self.src_nodes[i].port))
                    self._log.info(
                            " Failing over Destination Non-Master Node {0}:{1}".format(self.dest_nodes[j].ip, self.dest_nodes[j].port))
                    self._cluster_helper.failover(self.src_nodes, [self.src_nodes[i]])
                    self._cluster_helper.failover(self.dest_nodes, [self.dest_nodes[j]])
                    self._log.info(" Rebalance out Source Non-Master Node {0}:{1}".format(self.src_nodes[i].ip,
                                                                                          self.src_nodes[i].port))
                    self._log.info(" Rebalance out Destination Non-Master Node {0}:{1}".format(self.dest_nodes[i].ip,
                                                                                          self.dest_nodes[i].port))
                    tasks.extend(self._cluster_helper.rebalance(self.src_nodes, [], [self.src_nodes[i]]))
                    tasks.extend(self._cluster_helper.rebalance(self.dest_nodes, [], [self.dest_nodes[j]]))
                    self.src_nodes.remove(self.src_nodes[i])
                    removed_nodes.append(self.src_nodes[i])
                    self.dest_nodes.remove(self.dest_nodes[j])
                    removed_nodes.append(self.dest_nodes[j])
                    self._floating_servers_set.extend(removed_nodes)
                    self._test_count += 1
                else:
                    self._log.info("Not executing {0} as:".format(task_name))
                    if len(self.src_nodes) < 2:
                        self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                                    len(self.src_nodes)))
                    if len(self.dest_nodes) < 2:
                        self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on a cluster.".format(
                                    len(self.dest_nodes)))

            elif _r%7==0:
                task_name = "Rebalance_out_on_source"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                if len(self.src_nodes) > 1:
                    remove_node = self.src_nodes[len(self.src_nodes) - 1]
                    tasks.extend(self._async_rebalance(self.src_nodes, [], [remove_node]))
                    self._log.info(" Starting rebalance-out node {0} at Source cluster {1}".format(remove_node.ip,
                                                                                                   self.src_master.ip))
                    self.src_nodes.remove(remove_node)
                    self._floating_servers_set.extend([remove_node])
                    self._test_count += 1
                else:
                    self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on cluster.".format(
                                    len(self.src_nodes)))

            elif _r%11==0:
                task_name = "Rebalance_out_on_destination"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                if len(self.dest_nodes) > 1:
                    remove_node = self.dest_nodes[len(self.dest_nodes) - 1]
                    tasks.extend(self._async_rebalance(self.dest_nodes, [], [remove_node]))
                    self._log.info("Starting rebalance-out node {0} at Destination cluster {1}".format(remove_node.ip,
                                                                                                       self.dest_master.ip))
                    self.dest_nodes.remove(remove_node)
                    self._floating_servers_set.extend([remove_node])
                    self._test_count += 1
                else:
                    self._log.info("Number of nodes {0} is less than minimum '2' needed for failover on cluster".format(
                                    len(self.dest_nodes)))

            elif _r%5==0:
                task_name = "Swap_rebalance_one_on_source_and_destination"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                if len(self._floating_servers_set) < 2:
                    self._log.info("Number of nodes in floating server set less that the required 2")
                    continue
                add_node1 = self._floating_servers_set[0]
                add_node2 = self._floating_servers_set[1]
                if len(self.src_nodes) < 2 or len(self.dest_nodes) < 2:
                    continue

                remove_node1 = self.src_nodes[len(self.src_nodes) - 1]
                tasks.extend(self._async_rebalance(self.src_nodes, [add_node1], [remove_node1]))
                self._log.info("Starting swap-rebalance at Source cluster {0} add node {1} and remove node {2}".format(
                                self.src_master.ip, add_node1.ip, remove_node1.ip))
                self.src_nodes.remove(remove_node1)
                self.src_nodes.append(add_node1)
                self._floating_servers_set.append(remove_node1)

                remove_node2 = self.dest_nodes[len(self.dest_nodes) - 1]
                tasks.extend(self._async_rebalance(self.dest_nodes, [add_node2], [remove_node2]))
                self._log.info("Starting swap-rebalance at Destination cluster {0} add node {1} and remove node {2}".format(
                                self.dest_master.ip, add_node2.ip, remove_node2.ip))
                self.dest_nodes.remove(remove_node2)
                self.dest_nodes.append(add_node2)
                self._floating_servers_set.append(remove_node2)
                self._test_count += 1

            elif _r%19==0:
                task_name = "Reboot_all_destination"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                time.sleep(self._timeout / 6)
                for server in self.dest_nodes:
                    shell = RemoteMachineShellConnection(server)
                    o, r = shell.execute_command("sudo reboot")
                    shell.log_command_output(o, r)
                time.sleep(self._timeout * 3 / 2)
                self._test_count += 1

            elif _r%23==0:
                task_name = "Reboot_all_source"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                time.sleep(self._timeout / 6)
                for server in self.src_nodes:
                    shell = RemoteMachineShellConnection(server)
                    o, r = shell.execute_command("sudo reboot")
                    shell.log_command_output(o, r)
                time.sleep(self._timeout * 3 / 2)
                self._test_count += 1

            elif _r%41==0:
                task_name = "Stop_couchbase_servers_destination_restart_after_15min"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                time.sleep(self._timeout / 6)
                for server in self.dest_nodes:
                    shell = RemoteMachineShellConnection(server)
                    o, r = shell.execute_command("sudo /etc/init.d/couchbase-server stop")
                    shell.log_command_output(o, r)
                time.sleep(900)
                for server in self.dest_nodes:
                    shell = RemoteMachineShellConnection(server)
                    o, r = shell.execute_command("sudo /etc/init.d/couchbase-server start")
                    shell.log_command_output(o, r)
                time.sleep(self._timeout * 3 / 2)
                self._test_count += 1

            elif _r%43==0:
                task_name = "Stop_couchbase_servers_source_restart_after_15min"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                time.sleep(self._timeout / 6)
                for server in self.src_nodes:
                    shell = RemoteMachineShellConnection(server)
                    o, r = shell.execute_command("sudo /etc/init.d/couchbase-server stop")
                    shell.log_command_output(o, r)
                time.sleep(900)
                for server in self.src_nodes:
                    shell = RemoteMachineShellConnection(server)
                    o, r = shell.execute_command("sudo /etc/init.d/couchbase-server start")
                    shell.log_command_output(o, r)
                time.sleep(self._timeout * 3 / 2)
                self._test_count += 1

            elif _r%47==0:
                task_name = "Simulate_autofailover_on_source_&_destination_by_enabling_firewall"
                self._log.info(" - - - - - {0} - - - - - ".format(task_name))
                if len(self.src_nodes) < 2 or len(self.dest_nodes) < 2:
                    continue
                rest = RestConnection(self.src_master)
                rest.update_autofailover_settings(True, self._timeout / 2)
                rest = RestConnection(self.dest_master)
                rest.update_autofailover_settings(True, self._timeout / 2) 

                node = self.src_nodes[random.randrange(1, len(self.src_nodes))]
                self._enable_firewall(node)
                basesim.wait_for_failover_or_assert(self.src_master, 1, self._timeout)
                tasks.extend(self._cluster_helper.rebalance(self.src_nodes, [], [node]))
                self.src_nodes.remove(node)
                self._floating_servers_set(node)

                node = self.src_nodes[random.randrange(1, len(self.dest_nodes))]
                self._enable_firewall(node)
                basesim.wait_for_failover_or_assert(self.dest_master, 1, self._timeout)
                tasks.extend(self._cluster_helper.rebalance(self.dest_nodes, [], [node]))
                self.dest_nodes.remove(node)
                self._floating_servers_set(node)
                self._test_count += 1

            else:
                continue

            toss2 = random.randint(0,1000)
            if toss2 % 2 == 0:
                self._log.info("DELETE OPS IN PROGRESS ... ")
                src_buckets = self._get_cluster_buckets(self.src_master)
                for bucket in src_buckets:
                    valid_keys, deleted_keys = bucket.kvs[1].key_set()
                    item_count = len(valid_keys) + len(deleted_keys)
                g_delete1 = BlobGenerator('loadOne', 'loadOne-', self._value_size, 
                                          start=int((item_count) * (float)(100 - self._percent_delete) / 100), end=item_count)
                self._load_all_buckets(self.src_master, g_delete1, "delete", 0)    
                if self._replication_direction_str in "bidirection":
                    dest_buckets = self._get_cluster_buckets(self.dest_master)
                    for bucket in dest_buckets:
                        valid_keys, deleted_keys = bucket.kvs[1].key_set()
                        item_count = len(valid_keys) + len(deleted_keys)
                    g_delete2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, 
                                              start=int((item_count) * (float)(100 - self._percent_delete) / 100), end=item_count)
                    self._load_all_buckets(self.dest_master, g_delete2, "delete", 0)
                time.sleep(self._timeout)

            for task in tasks:
                task.result()

            task_done = datetime.datetime.now()
            self._log.info("===> NO. OF TESTS RUN SO FAR: {0}".format(self._test_count))
            str = "TASK: {0}:: RUN_TIME: {1} seconds :: with update-ops: {2} and delete-ops: {3}".format(task_name, ((task_done - task_begin).seconds), ((toss1%2)!=1), ((toss2%2)!=1))
            self._log.info(str)
            self._output_.extend([str])
            self._log.info("UPTIME: {0} seconds".format((task_done - start_time).seconds))
            time.sleep(self._timeout)
            #STOP RUN AFTER 2 HOURS OR 10 TESTS, WHICHEVER COMES FIRST
            if (task_done - start_time).seconds > 7200 or self._test_count >= 10:
                self._log.info("Run time: {0} hours :: Stopping now!...".format((float)((task_done - start_time).seconds)/3600))
                break

        if self._replication_direction_str in "unidirection":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
            self.verify_results()
        elif self._replication_direction_str in "bidirection":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
            self.verify_results(verify_src=True)

        fin_time = datetime.datetime.now()
        self._log.info(" = = = = = = = = = = = = = = = RESULTS = = = = = = = = = = = = = = = ")
        self._log.info("XDCR: {0} ; TOPOLOGY: {1}".format(self._replication_direction_str, self._cluster_topology_str))
        for i in range(len(self._output_)):
            self._log.info(self._output_[i])
        self._log.info("TOTAL RUN TIME: {0} seconds".format((fin_time - start_time)))
        self._log.info(" = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = ")
