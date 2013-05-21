import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseMetaClient;
import com.couchbase.client.MetaData;

public class Delrunner {

    public static void dels (Stronghold sh, CouchbaseMetaClient _sclient, CouchbaseMetaClient _dclient) {
	/*
	 * Method to delete items through delrms' on the source cluster,
	 * and with the retrieved metaData runs delwithmetas' on the
	 * destination cluster
	 */
	for (int i=sh.getItemcount(); i<(sh.getItemcount() + sh.getAddCount()); i++) {
	    OperationFuture<MetaData> delrm;
	    String key = String.format("%s%d", sh.getPrefix(), i);
	    delrm = _sclient.deleteReturnMeta(key, 0);
	    if (delrm.isDone()) {
		try {
		    _dclient.deleteWithMeta(key, delrm.get(), 0);
		} catch (Exception e) {
		    // Do nothing, client will not care whether these deletewithmetas' actually went through
		}
	    }
	}
    }
}
