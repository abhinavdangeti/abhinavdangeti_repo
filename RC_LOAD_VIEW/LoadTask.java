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
			
	private static int NUM_ITEMS = 0;
	private static int EXPIRES = 0;
	private static String BUCKET_NAME = "default";
	private static String BUCKET_PASSWD = "";
	private static String port = "8091";
	private static String serverAddr = "127.0.0.1";
	private static String do_delete_flag = "No";

	
	 static final CouchbaseClient connect() throws URISyntaxException, IOException{
		List<URI> uris = new LinkedList<URI>();
		uris.add(URI.create(String.format("http://" + serverAddr + ":" + port + "/pools")));
	    
	    try {
	      return new CouchbaseClient(uris, BUCKET_NAME, BUCKET_PASSWD);
	    } catch (Exception e) {
	      System.err.println("Error connecting to Couchbase: "
	        + e.getMessage());
	      System.exit(0);
	    }
	    return null;
	    
	}
	
	private static void load_items() throws InterruptedException, ExecutionException, URISyntaxException, IOException{
		CouchbaseClient client = connect();
		for(int i=1;i<=NUM_ITEMS;i++){
			String Key = String.format("Key-%d", i);
			String Value = String.format("%d", i);
			OperationFuture<Boolean> setOp = client.set(Key, EXPIRES, Value);
			if (setOp.get().booleanValue() == false) {
				System.err.println("Set failed: " + 
							setOp.getStatus().getMessage());
			    break;
			} else {
				System.out.println("Set Key: " + i);
			}		
		}
	}
	
	private static void delete_items(CouchbaseClient client){
		for(int i=1;i<=NUM_ITEMS;i++){
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
		
		CouchbaseClient client = null;
		
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
			client = connect();
		}catch (IOException ex){
			ex.printStackTrace();
		}
		
		final int ITEM_COUNT = NUM_ITEMS;
		
		Runnable myRunnable1 = new Runnable() {
			@Override
			public void run() {
				System.out.println("Load a " + ITEM_COUNT + " items ..");
				try {
					load_items();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (URISyntaxException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}			
		};

		Runnable myRunnable2 = new Runnable() {		
			public void run() {
				CouchbaseClient client1 = null;
				try{
					client1 = connect();
					} catch(Exception e){
					}				
				while(true){
					int count = 0;
					for(int i=1;i<=ITEM_COUNT;i++){
						Object getObject = null;
						try{
							getObject = client1.get(String.format("Key-%d", i));
						}catch (Exception e){
							break;
						}
						if (getObject != null) {
							System.out.println("Retrieved: "+ (String) getObject);
						    count++;
						} else {
							System.err.println("Synchronous Get failed");
							break;
						}
					}
					if(count==ITEM_COUNT){
						System.out.println("Synchronous Get for all " + count + " items : Suceeded! ");
						client1.shutdown(10, TimeUnit.SECONDS);
						break;
					}
				}
			}
		};
		
		System.out.println("Running thread1: ");
		Thread thread1 = new Thread(myRunnable1);
		thread1.start();	
		System.out.println("Running thread2: ");
		Thread thread2 = new Thread(myRunnable2);
		thread2.start();
		thread1.join();
		thread2.join();
		
		if (do_delete_flag.equals("Yes") || do_delete_flag.equals("1")) {
			System.out.println("Now deleting the " + NUM_ITEMS + " items ..");
			delete_items(client);
		}
		
		client.shutdown(10, TimeUnit.SECONDS);
		System.exit(0);
	}
}
