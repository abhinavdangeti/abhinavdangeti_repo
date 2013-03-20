import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;


public class Appender {

	public static void append_items (int _itemCount, double _appendRatio, int _appendSize, boolean _json, CouchbaseClient client, String _prefix) 
			throws URISyntaxException, IOException, JSONException, InterruptedException, ExecutionException {
		double itemstoappend = _appendRatio * _itemCount;
		StringBuffer value = new StringBuffer();
		String CHAR_LIST = "!@#$%^&*()";
        while (value.length() < _appendSize) {
           value.append(CHAR_LIST);
        }
        
        List<OperationFuture<Boolean>> appends = new LinkedList<OperationFuture<Boolean>>();
		for (int i=0; i<itemstoappend; i++) {
			OperationFuture<Boolean> appendOp;
			String key = String.format("%s%d", _prefix, i);
			if (_json) {
				JSONObject _val = Spawner.appendJSON(i, _appendSize);
				appendOp = client.append(0, key, _val.toString());
			} else {
				appendOp = client.append(0, key, value.toString());
				appends.add(appendOp);
			}
			
		}
		while (!appends.isEmpty()) {
			if (appends.get(0).get().booleanValue() == false){
				System.err.println("Append failed: "/* + appendOp.getStatus().getMessage()*/);
				continue;
			}
			appends.remove(0);
		}
	}
}
