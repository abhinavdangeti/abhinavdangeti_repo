import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;


public class Appender {

	public static void append_items (int _itemCount, double _appendRatio, int _appendSize, boolean _json) 
			throws URISyntaxException, IOException, JSONException, InterruptedException, ExecutionException {
		CouchbaseClient client = Loadrunner.connect();
		double itemstoappend = _appendRatio * _itemCount;
		StringBuffer value = new StringBuffer();
		String CHAR_LIST = "0000000000";
        while (value.length() < _appendSize) {
           value.append(CHAR_LIST);
        }
        OperationFuture<Boolean> appendOp = null;
		for (int i=0; i<itemstoappend; i++) {
			String key = String.format("Key-%d", i);
			if (_json) {
				JSONObject _val = Spawner.appendJSON(i, _appendSize);
				appendOp = client.append(0, key, _val.toString());
			} else {
				appendOp = client.append(0, key, value.toString());
			}
			if (appendOp.get().booleanValue() == false){
				System.err.println("Append failed: " + appendOp.getStatus().getMessage());
				continue;
			}
		}
		client.shutdown();	
	}
}
