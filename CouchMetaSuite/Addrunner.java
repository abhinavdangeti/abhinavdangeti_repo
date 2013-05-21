import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseMetaClient;
import com.couchbase.client.MetaData;

public class Addrunner {

	public static void adds (Stronghold sh, CouchbaseMetaClient _sclient, CouchbaseMetaClient _dclient) 
			throws JSONException {
		/*
		 * Module to create items through addrms' on the source cluster,
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
		for (int i=sh.getItemcount(); i<(sh.getItemcount() + sh.getAddCount()); i++) {
			OperationFuture<MetaData> addrm;
			String key = String.format("%s%d", sh.getPrefix(), i);
			if (sh.isJson()) {
				JSONObject _val = Spawner.retrieveJSON(gen, sh.getItemsize());
				addrm = _sclient.addReturnMeta(key, 0, _val.toString());
				if (addrm.isDone()) {
					try {
						_dclient.setWithMeta(key, _val.toString(), addrm.get(), 0);
					} catch (Exception e) {
						// Do nothing, client will not care whether these addwithmetas' actually went through
					}
				}
			} else {
				addrm = _sclient.addReturnMeta(key, 0, value.toString());
				if (addrm.isDone()) {
					try {
						_dclient.setWithMeta(key, value.toString(), addrm.get(), 0);
					} catch (Exception e) {
						// Do nothing, client will not care whether these addwithmetas' actually went through
					}
				}
			}
			creates.add(addrm);
		}

		while (!creates.isEmpty()) {
			if (creates.get(0).isDone() == false){
			    System.err.println("Add failed");
			    continue;
			}
			creates.remove(0);
		}
	}
}
