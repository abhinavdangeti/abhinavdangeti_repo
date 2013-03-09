from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from xdcrbasetests import XDCRReplicationBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from random import randrange

import time

class test(XDCRReplicationBaseTest):
    def setUp(self):
        super(test, self).setUp()

        self.gen_create2 = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self._num_items)
        self.gen_delete2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size,
            start=int((self._num_items) * (float)(100 - self._percent_delete) / 100), end=self._num_items)
        self.gen_update2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, start=0,
            end=int(self._num_items * (float)(self._percent_update) / 100))

    def tearDown(self):
        super(test, self).tearDown()
        
    def load_with_ops_and_backup_restore(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        raw_input("LOAD COMPLETED ON SOURCE, press ENTER to continue..")

        tasks = []
        if self._doc_ops is not None:
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        shell1 = RemoteMachineShellConnection(self.src_master)
        o, r = shell1.execute_command("sudo rm -rf ~/backup")
        shell1.log_command_output(o, r)
        time.sleep(self._timeout / 4)
        o, r = shell1.execute_command("/opt/couchbase/bin/cbbackup http://localhost:8091 ~/backup")
        shell1.log_command_output(o, r)
        shell2 = RemoteMachineShellConnection(self.dest_master)
        o, r = shell2.execute_command("sudo rm -rf ~/backup")
        shell2.log_command_output(o, r)
        time.sleep(self._timeout / 4)
        o, r = shell2.execute_command("/opt/couchbase/bin/cbbackup http://localhost:8091 ~/backup")
        shell2.log_command_output(o, r)

        for task in tasks:
            task.result()

        restore = raw_input("Flush items at will, once done CBRESTORE will be called, type source or destination or both to do cbrestore .. ")
        if restore == "source":
            o, r = shell1.execute_command("/opt/couchbase/bin/cbrestore ~/backup http://localhost:8091")
            shell1.log_command_output(o, r)
            time.sleep(self._timeout * 2)
            self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
            time.sleep(self._timeout / 2)
            self.verify_results(verify_src=True)
        elif restore == "destination":
            o, r = shell2.execute_command("/opt/couchbase/bin/cbrestore ~/backup http://localhost:8091")
            shell2.log_command_output(o, r)
            time.sleep(self._timeout)
            self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
            time.sleep(self._timeout / 2)
            self.verify_results(verify_src=True)
        elif restore == "both":
            o, r = shell1.execute_command("/opt/couchbase/bin/cbrestore ~/backup http://localhost:8091")
            shell1.log_command_output(o, r)
            o, r = shell2.execute_command("/opt/couchbase/bin/cbrestore ~/backup http://localhost:8091")
            shell2.log_command_output(o, r)
            time.sleep(self._timeout)
            self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
            time.sleep(self._timeout / 2)
            self.verify_results(verify_src=True)
        else:
            print "cbrestore not executed"
    
    def load_with_ops_bidirectional_and_backup_restore(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)
        raw_input("LOAD COMPLETED ON SOURCES, press ENTER to continue..")

        tasks = []
        if self._doc_ops is not None:
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        if self._doc_ops_dest is not None:
            if "update" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_update2, "update", self._expires))
            if "delete" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0))

        shell1 = RemoteMachineShellConnection(self.src_master)
        o, r = shell1.execute_command("sudo rm -rf ~/backup")
        shell1.log_command_output(o, r)
        time.sleep(self._timeout / 4)
        o, r = shell1.execute_command("/opt/couchbase/bin/cbbackup http://localhost:8091 ~/backup")
        shell1.log_command_output(o, r)
        shell2 = RemoteMachineShellConnection(self.dest_master)
        o, r = shell2.execute_command("sudo rm -rf ~/backup")
        shell2.log_command_output(o, r)
        time.sleep(self._timeout / 4)
        o, r = shell2.execute_command("/opt/couchbase/bin/cbbackup http://localhost:8091 ~/backup")
        shell2.log_command_output(o, r)

        for task in tasks:
            task.result()

        restore = raw_input("Flush items at will, once done CBRESTORE will be called, type source or destination or both to do cbrestore .. ")
        if restore == "source":
            o, r = shell1.execute_command("/opt/couchbase/bin/cbrestore ~/backup http://localhost:8091")
            shell1.log_command_output(o, r)
            time.sleep(self._timeout)
            self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
            time.sleep(self._timeout / 2)
            self.verify_results(verify_src=True)
        elif restore == "destination":
            o, r = shell2.execute_command("/opt/couchbase/bin/cbrestore ~/backup http://localhost:8091")
            shell2.log_command_output(o, r)
            time.sleep(self._timeout)
            self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
            time.sleep(self._timeout / 2)
            self.verify_results(verify_src=True)
        elif restore == "both":
            o, r = shell1.execute_command("/opt/couchbase/bin/cbrestore ~/backup http://localhost:8091")
            shell1.log_command_output(o, r)
            o, r = shell2.execute_command("/opt/couchbase/bin/cbrestore ~/backup http://localhost:8091")
            shell2.log_command_output(o, r)
            time.sleep(self._timeout)
            self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
            time.sleep(self._timeout / 2)
            self.verify_results(verify_src=True)
        else:
            print "cbrestore not executed"

    #Bidirectional replication only
    def testing_conflicts(self):
        rest1 = RestConnection(self.src_master)
        rest1.remove_all_replications()
        rest2 = RestConnection(self.dest_master)
        rest2.remove_all_replications()
        raw_input("Press Enter to continue ..")
       
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        raw_input("Creation phase complete, press Enter to continue ..")
        tasks = tasks1 = []
        s_upd_count = randrange(0, 5)
        for i in range(0, 3):
            print "No. of times updated by source .. " + str(i+1) 
            if "update" in self._doc_ops:
                tasks1 += self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires)
        for task in tasks1:
            task.result()
        if "update" in self._doc_ops and "update" in self._doc_ops_dest:
            self.sleep(30)
        raw_input("Initial update phase by source complete, press Enter to continue and set up the replications ..")
        self._replicate_clusters(self.src_master, self._cluster_names_dic[self._clusters_keys_olst[1]])
        self._replicate_clusters(self.dest_master, self._cluster_names_dic[self._clusters_keys_olst[0]])
        raw_input("Are the replications set up?, press Enter to continue with waiting for replications to catch up ..")
        self.sleep(self._timeout * 2)
        raw_input("Press Enter to continue with updates from destination ..")
        d_upd_count = randrange(0, 3)
        for i in range(0, 3):
            print "No. of times updated by destination .. " + str(i+1)
            if "update" in self._doc_ops_dest:
                tasks += self._async_load_all_buckets(self.dest_master, self.gen_update, "update", self._expires)
        raw_input("Press Enter to have the final update from the source ..")
        tasks += self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires)
        
        if "delete" in self._doc_ops:
            tasks += self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0)
        if "delete" in self._doc_ops_dest:
            tasks += self._async_load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0)

        for task in tasks:
            task.result()

        raw_input("Press Enter to begin merging keys on KVStore and then verify .. ")
        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.verify_results(verify_src=True)
