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


public class Deleter {

	public static void del_items (int _itemCount, CouchbaseClient client, String _prefix) 
			throws URISyntaxException, IOException, JSONException, InterruptedException, ExecutionException {
		for (int i=0; i<_itemCount; i++) {
			OperationFuture<Boolean> delOp;
			String key = String.format("%s%d", _prefix, i);
			delOp = client.delete(key);
		}
	}
}
