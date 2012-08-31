import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;


public class Viewer {
	
	/*
	 * Querys an already created view.
	 */
	public static boolean loadQuery(String ddocname, String viewname, String serverAddr, String port) 
			throws URISyntaxException, IOException, InterruptedException, ExecutionException {
	
		String SERVER_URI = "http://" + serverAddr + ":" + port + "/pools";
		List<URI> uris = new LinkedList<URI>();
		uris.add(URI.create(SERVER_URI));
		CouchbaseClient client = Mainhelper.connect();
		
		View view = client.getView(ddocname, viewname);
		if (view == null){
			System.out.println(" - - VIEW ==> NULL - - ");
			client.shutdown();
			return false;
		}
		
		Query query = new Query();
		query.setReduce(false);
		query.setStale(Stale.FALSE);
		
		ViewResponse response = client.asyncQuery(view, query).get();
		
		Iterator<ViewRow> itr = response.iterator();
		ViewRow row;
		int i = 0;
		while(itr.hasNext()){
			row = itr.next();
			i ++;
			System.out.println("Found: "+ i + " - " + row.getKey() + " :: " + row.getValue());
		}
		
		client.shutdown();
		return true;
		
	}
}
