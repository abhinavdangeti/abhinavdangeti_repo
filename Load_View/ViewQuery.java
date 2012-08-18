import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;

public class ViewQuery {
	
	private static CouchbaseClient client;
	
	@SuppressWarnings("rawtypes")
	public static void main(String args[]) throws InterruptedException, ExecutionException{
		
		String BUCKET_NAME = "default", BUCKET_PASSWD = "", port = "8091", serverAddr = "127.0.0.1";
		String ddocname = "", viewname = "";
		
		try {
			File file = new File("test.properties");
			FileInputStream fileInput = new FileInputStream(file);
			Properties properties = new Properties();
			properties.load(fileInput);
			fileInput.close();
			
			Enumeration enuKeys = properties.keys();
			while(enuKeys.hasMoreElements()){
				String key = (String) enuKeys.nextElement();
				if(key.equals("bucket-name"))
					BUCKET_NAME = properties.getProperty(key);
				else if(key.equals("bucket-password"))
					BUCKET_PASSWD = properties.getProperty(key);
				else if(key.equals("servers"))
					serverAddr = properties.getProperty(key);
				else if(key.equals("port"))
					port = properties.getProperty(key);	
				else if(key.equals("ddoc-name"))
					ddocname = properties.getProperty(key);
				else if(key.equals("view-name"))
					viewname = properties.getProperty(key);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Result of load is " + loadQuery(BUCKET_NAME, BUCKET_PASSWD, 
													ddocname, viewname, serverAddr, port));
	}

	private static boolean loadQuery(String bucketname, String bucketpasswd, String ddocname,
			String viewname, String serverAddr, String port) throws InterruptedException, ExecutionException {
	
		String SERVER_URI = "http://" + serverAddr + ":" + port + "/pools";
		List<URI> uris = new LinkedList<URI>();
		uris.add(URI.create(SERVER_URI));
		try {
			client = new CouchbaseClient(uris, bucketname, bucketpasswd);
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
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

