import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseMetaClient;
import com.couchbase.client.MetaData;

public class Setrunner {

    public static void sets (Stronghold sh, CouchbaseMetaClient _sclient, CouchbaseMetaClient _dclient) 
	throws JSONException {
	    /*
	     * Module to create items through setrms' on the source cluster,
	     * and with the retrieved metaData, runs setwithmetas' on the
	     * destination cluster
	     */
	    Random gen = new Random ( 987654321 );
	    StringBuffer value = new StringBuffer();
	    String CHAR_LIST = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	    while (value.length() < sh.getItemsize()) {
		value.append(CHAR_LIST);
	    }

	    List<OperationFuture<MetaData>> creates = new LinkedList<OperationFuture<MetaData>>();
	    for (int i=0; i<(sh.getItemcount() - (int)((double)(sh.getItemcount()) * sh.getExpRatio()) - 1); i++) {
		OperationFuture<MetaData> setrm;
		String key = String.format("%s%d", sh.getPrefix(), i);
		if (sh.isJson()) {
		    JSONObject _val = Spawner.retrieveJSON(gen, sh.getItemsize());
		    setrm = _sclient.setReturnMeta(key, 0, 0, _val.toString());
		    if (setrm.isDone()) {
			try {
			    _dclient.setWithMeta(key, _val.toString(), setrm.get(), 0);
			} catch (Exception e) {
			    // Do nothing, client will not care whether these setwithmetas' actually went through
			}
		    }
		} else {
		    setrm = _sclient.setReturnMeta(key, 0, 0, value.toString());
		    if (setrm.isDone()) {
			try {
			    _dclient.setWithMeta(key, value.toString(), setrm.get(), 0);
			} catch (Exception e) {
			    // Do nothing, client will not care whether these setwithmetas' actually went through
			}
		    }
		}
		creates.add(setrm);
	    }
	    for (int i=(sh.getItemcount() - (int)((double)(sh.getItemcount()) * sh.getExpRatio())); i<sh.getItemcount(); i++) {
		OperationFuture<MetaData> setrm;
		String key = String.format("%s%d", sh.getPrefix(), i);
		if (sh.isJson()) {
		    JSONObject _val = Spawner.retrieveJSON(gen, sh.getItemsize());
		    setrm = _sclient.setReturnMeta(key, sh.getExpiration(), 0, _val.toString());
		    if (setrm.isDone()) {
			try {
			    _dclient.setWithMeta(key, _val.toString(), setrm.get(), 0);
			} catch (Exception e) {
			    // Do nothing, client will not care whether these setwithmetas' actually went through
			}
		    }
		} else {
		    setrm = _sclient.setReturnMeta(key, sh.getExpiration(), 0, value.toString());
		    if (setrm.isDone()) {
			try {
			    _dclient.setWithMeta(key, value.toString(), setrm.get(), 0);
			} catch (Exception e) {
			    // Do nothing, client will not care whether these setwithmetas' actually went through
			}
		    }
		}
		creates.add(setrm);
	    }

	    while (!creates.isEmpty()) {
		if (creates.get(0).isDone() == false){
		    System.err.println("Set failed");
		    continue;
		}
		creates.remove(0);
	    }
	}
}
