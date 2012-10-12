from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from xdcrbasetests import XDCRReplicationBaseTest
from remote.remote_util import RemoteMachineShellConnection

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
