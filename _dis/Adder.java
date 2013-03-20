import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.internal.OperationFuture;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.couchbase.client.CouchbaseClient;


public class Adder {

	public static void add_items (int _itemCount, int _itemSize, int _addMore, boolean _json, CouchbaseClient client, String _prefix) 
			throws URISyntaxException, IOException, JSONException, InterruptedException, ExecutionException {
		Random gen = new Random (123456789);
		StringBuffer value = new StringBuffer();
		String CHAR_LIST = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        while (value.length() < _itemSize) {
           value.append(CHAR_LIST);
        }
        
        List<OperationFuture<Boolean>> adds = new LinkedList<OperationFuture<Boolean>>();
		for (int i=_itemCount; i<(_itemCount+_addMore); i++) {
			OperationFuture<Boolean> addOp;
			String key = String.format("%s%d", _prefix, i);
			if (_json) {
				JSONObject _val = Spawner.retrieveJSON(gen, _itemSize);
				addOp = client.set(key, 0, _val.toString());
			} else {
				addOp = client.set(key, 0, value.toString());
				adds.add(addOp);
			}
		}
		while (!adds.isEmpty()) {
			if (adds.get(0).get().booleanValue() == false){
				System.err.println("Add/Set failed: "/* + addOp.getStatus().getMessage()*/);
				continue;
			}
			adds.remove(0);
		}
	}
}
