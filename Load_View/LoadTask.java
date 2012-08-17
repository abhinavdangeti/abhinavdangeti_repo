import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.File;

import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.internal.OperationFuture;

public class LoadTask {

	private static CouchbaseClient client;
	
	private static void connect(String serverAddr, String port, String BUCKET_NAME, String BUCKET_PASSWD) throws URISyntaxException, IOException{
		
		List<URI> uris = new LinkedList<URI>();
		uris.add(URI.create(String.format("http://" + serverAddr + ":" + port + "/pools")));
	    
	    try {
	      client = new CouchbaseClient(uris, BUCKET_NAME, BUCKET_PASSWD);
	    } catch (Exception e) {
	      System.err.println("Error connecting to Couchbase: "
	        + e.getMessage());
	      System.exit(0);
	    }
	}
	
	private static void load_items(int count, int EXPIRES) throws InterruptedException, ExecutionException{
		for(int i=1;i<=count;i++){
			String Key = String.format("Key-%d", i);
			String Value = String.format("%d", i);
			OperationFuture<Boolean> setOp = client.set(Key, EXPIRES, Value);
			if (setOp.get().booleanValue() == false) {
				System.err.println("Set failed: " + 
							setOp.getStatus().getMessage());
			    break;
			}
		}		
	}
	
	private static void delete_items(int count){
		for(int i=1;i<=count;i++){
			try {
		        OperationFuture<Boolean> delOp = client.delete(String.format("Key-%d", i));;
				if (delOp.get().booleanValue() == false) {
					System.err.println("Delete failed: " +
							delOp.getStatus().getMessage());
		        }
		      } catch (Exception e) {
		        System.err.println("Exception while doing delete: "
		            + e.getMessage());
		      }
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static void main(String args[]) throws URISyntaxException, InterruptedException, ExecutionException{
		
		int NUM_ITEMS = 0, EXPIRES = 0;
		String BUCKET_NAME = "default", BUCKET_PASSWD = "", port = "8091", serverAddr = "127.0.0.1", do_delete_flag = "No";
		
		try {
			File file = new File("test.properties");
			FileInputStream fileInput = new FileInputStream(file);
			Properties properties = new Properties();
			properties.load(fileInput);
			fileInput.close();
			
			Enumeration enuKeys = properties.keys();
			while(enuKeys.hasMoreElements()){
				String key = (String) enuKeys.nextElement();
				if(key.equals("item-count"))
					NUM_ITEMS = Integer.parseInt(properties.getProperty(key));
				else if(key.equals("bucket-name"))
					BUCKET_NAME = properties.getProperty(key);
				else if(key.equals("bucket-password"))
					BUCKET_PASSWD = properties.getProperty(key);
				else if(key.equals("expires"))
					EXPIRES = Integer.parseInt(properties.getProperty(key));
				else if(key.equals("servers"))
					serverAddr = properties.getProperty(key);
				else if(key.equals("port"))
					port = properties.getProperty(key);	
				else if(key.equals("do-delete"))
					do_delete_flag = properties.getProperty(key);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try{
			connect(serverAddr, port, BUCKET_NAME, BUCKET_PASSWD);
		}catch (IOException ex){
			ex.printStackTrace();
		}
		
		System.out.println("Load a " + NUM_ITEMS + " items ..");
		load_items(NUM_ITEMS, EXPIRES);
		
		int count = 0;
		for(int i=1;i<=NUM_ITEMS;i++){
			Object getObject = client.get(String.format("Key-%d", i));
			if (getObject != null) {
				//System.out.println("Retrieved: "+ (String) getObject);
			    count++;
			} else {
				System.err.println("Synchronous Get failed");
				break;
			}
		}
		
		if(count==NUM_ITEMS)
			System.out.println("Synchronous Get for all " + count + " items : Suceeded! ");
		
		if (do_delete_flag.equals("Yes") || do_delete_flag.equals("1")) {
			System.out.println("Now deleting the " + NUM_ITEMS + " items ..");
			delete_items(NUM_ITEMS);
		}
		
		client.shutdown(10, TimeUnit.SECONDS);
	}
}
