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
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;

import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.internal.OperationFuture;

public class LoadTaskWithObserve {
			
	private static int NUM_ITEMS = 0;
	private static int EXPIRATION = 0;
	private static double RATIO_EXP = 0;
	private static String BUCKET_NAME = "default";
	private static String BUCKET_PASSWD = "";
	private static String port = "8091";
	private static String serverAddr = "127.0.0.1";
	private static String do_delete_flag = "No";
	private static double DEL_PERCENT = 0.0;
	
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
		int ITEMS_WITH_EXP = (int) (NUM_ITEMS * RATIO_EXP);
		//System.out.println(ITEMS_WITH_EXP);
		for(int i=1;i<=(NUM_ITEMS - ITEMS_WITH_EXP);i++){
			String Key = String.format("Key-%d", i);
			String Value = String.format("%d", i);
			OperationFuture<Boolean> setOp = client.set(Key, 0, Value, PersistTo.MASTER);
			assert setOp.get().booleanValue() : "Key was not persisted by master";
			//setOp = client.set(Key, 0, Value, PersistTo.FOUR, ReplicateTo.THREE);
			//assert !setOp.get().booleanValue() : "Were there really 4 servers with 3 replicas for a testing system ?";
			if (setOp.get().booleanValue() == false) {
				System.err.println("Set failed: " + 
							setOp.getStatus().getMessage());
			    break;
			} else {
				//System.out.println("Set Key: " + i);
			}		
		}
		for(int i=(NUM_ITEMS - ITEMS_WITH_EXP + 1);i<=NUM_ITEMS;i++){
			String Key = String.format("Key-%d", i);
			String Value = String.format("%d", i);
			OperationFuture<Boolean> setOp = client.set(Key, EXPIRATION, Value, PersistTo.MASTER);
			assert setOp.get().booleanValue() : "Key was not persisted by master";
			//setOp = client.set(Key, EXPIRATION, Value, PersistTo.FOUR, ReplicateTo.THREE);
			//assert !setOp.get().booleanValue() : "Were there really 4 servers with 3 replicas for a testing system ?";
			if (setOp.get().booleanValue() == false) {
				System.err.println("Set failed: " + 
							setOp.getStatus().getMessage());
			    break;
			} else {
				//System.out.println("Set Key: " + i);
			}		
		}
		client.shutdown(10, TimeUnit.SECONDS);
	}
	
	private static void get_items() throws URISyntaxException, IOException {
		CouchbaseClient client = connect();
		while(true){
			int count = 0;
			for(int i=1;i<=NUM_ITEMS;i++){
				Object getObject = null;
				try{
					getObject = client.get(String.format("Key-%d", i));
				}catch (Exception e){
					break;
				}
				if (getObject != null) {
					//System.out.println("Retrieved: "+ (String) getObject);
				    count++;
				} else {
					//System.err.println("Synchronous Get failed");
					break;
				}
			}
			if((count==NUM_ITEMS) || (count==(NUM_ITEMS - (DEL_PERCENT * NUM_ITEMS)))){
				System.out.println("Synchronous Get for all " + count + " items : Suceeded! ");
				client.shutdown(10, TimeUnit.SECONDS);
				break;
			}
		}
	}
	
	private static void delete_items() throws URISyntaxException, IOException{
		double del_items = DEL_PERCENT * NUM_ITEMS;
		CouchbaseClient client = connect();
		int count = 0;
		while(true){
			for(int i=1;i<=(int)(del_items);i++){
				try {
					OperationFuture<Boolean> delOp = client.delete(String.format("Key-%d", i), PersistTo.MASTER);
					assert delOp.get().booleanValue() : "Key was not persisted to master";
					
					if (delOp.get().booleanValue() == false) {
						System.err.println("Delete failed: " +
								delOp.getStatus().getMessage());
						break;
					} else {
						count ++;
					}
				} catch (Exception e) {
					System.err.println("Exception while doing delete: "
							+ e.getMessage());
				}
			}
			//System.out.println(count + "/" + (int)(del_items));
			if(count == (int)(del_items)){
				System.out.println("Items deleted: " + count);
				client.shutdown(10, TimeUnit.SECONDS);
				break;
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static void main(String args[]) throws URISyntaxException, InterruptedException, ExecutionException, IOException{
		
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
				else if(key.equals("expiration"))
					EXPIRATION = Integer.parseInt(properties.getProperty(key));
				else if(key.equals("ratio-expires"))
					RATIO_EXP = Float.parseFloat(properties.getProperty(key));
				else if(key.equals("servers"))
					serverAddr = properties.getProperty(key);
				else if(key.equals("port"))
					port = properties.getProperty(key);	
				else if(key.equals("do-delete"))
					do_delete_flag = properties.getProperty(key);
				else if(key.equals("ratio-deletes"))
					DEL_PERCENT = Float.parseFloat(properties.getProperty(key));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Runnable myRunnable1 = new Runnable() {
			public void run() {
				System.out.println("Load a " + NUM_ITEMS + " items ..");
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
				try {
					get_items();
				} catch (URISyntaxException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		
		Runnable myRunnable3 = new Runnable() {
			public void run() {
				if (do_delete_flag.equals("Yes") || do_delete_flag.equals("1")) {
					try {
						delete_items();
					} catch (URISyntaxException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
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
		thread2.join();
		System.out.println("Running thread3: ");
		Thread thread3 = new Thread(myRunnable3);
		thread3.start();
		
		thread1.join();
		thread3.join();
		
		System.exit(0);
	}
}
