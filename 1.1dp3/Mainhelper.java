import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.CouchbaseClient;


public class Mainhelper {

	 private static int NUM_ITEMS = 0;
	 private static int EXPIRATION = 0;
	 private static double RATIO_EXP = 0;
	 private static Boolean OBSERVE = false;
	 private static String BUCKET_NAME = "default";
	 private static String BUCKET_PASSWD = "";
	 private static String port = "8091";
	 private static String serverAddr = "127.0.0.1";
	 private static String do_delete_flag = "No";
	 private static double DEL_PERCENT = 0.0;
	 private static String do_replace_flag = "No";
	 private static double REPLACE_PERCENT = 0.0;
	 private static String do_add_flag = "No";
	 private static double ADD_PERCENT = 0.0;
	 private static String ddoc_name = "";
	 private static String view_name = "";
	
	 /*
	  * Establish connection with couchbase server
	  */
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
	 
	 /*
	  * Read Input parameters from test.properties
	  * Run threads for set and get simultaneously. Wait for completion.
	  * Run threads for delete, replace, add simultaneously. Wait for completion.
	  * Query an already created view.
	  */
	 @SuppressWarnings("rawtypes")
	 public static void main(String args[]) throws InterruptedException, URISyntaxException, IOException, ExecutionException {
		 
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
				else if(key.equals("observe"))
					OBSERVE = Boolean.parseBoolean(properties.getProperty(key));
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
				else if(key.equals("do-replace"))
					do_replace_flag = properties.getProperty(key);
				else if(key.equals("replace-ratio"))
					REPLACE_PERCENT = Float.parseFloat(properties.getProperty(key));
				else if(key.equals("do-add"))
					do_add_flag = properties.getProperty(key);
				else if(key.equals("add-ratio"))
					ADD_PERCENT = Float.parseFloat(properties.getProperty(key));
				else if(key.equals("ddoc-name"))
					ddoc_name = properties.getProperty(key);
				else if(key.equals("view-name"))
					view_name = properties.getProperty(key);
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
					Loader.load_items(NUM_ITEMS, RATIO_EXP, EXPIRATION, OBSERVE);
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
				} catch (URISyntaxException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		
		Runnable myRunnable3 = new Runnable() {
			public void run() {
				if (do_delete_flag.equals("yes") || do_delete_flag.equals("1")) {
					try {
						Deleter.delete_items(NUM_ITEMS, DEL_PERCENT, OBSERVE);
					} catch (URISyntaxException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		Runnable myRunnable4 = new Runnable() {
			public void run() {
				if (do_replace_flag.equals("yes") || do_replace_flag.equals("1")){
					try {
						Replacer.replace_items(NUM_ITEMS, REPLACE_PERCENT, EXPIRATION, OBSERVE);
					} catch (URISyntaxException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		Runnable myRunnable5 = new Runnable() {
			public void run() {
				if (do_add_flag.equals("yes") || do_add_flag.equals("1")){
					try {
						Adder.add_item(NUM_ITEMS, ADD_PERCENT, EXPIRATION, OBSERVE);
					} catch (URISyntaxException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		};
		
		System.out.println("Running thread to set: ");
		Thread thread1 = new Thread(myRunnable1);
		thread1.start();
		System.out.println("Running thread to get: ");
		Thread thread2 = new Thread(myRunnable2);
		thread2.start();
		
		thread1.join();
		thread2.join();
		
		System.out.println("Running thread to delete: ");
		Thread thread3 = new Thread(myRunnable3);
		thread3.start();
		System.out.println("Running thread to replace: ");
		Thread thread4 = new Thread(myRunnable4);
		thread4.start();
		System.out.println("Running thread to add: ");
		Thread thread5 = new Thread(myRunnable5);
		thread5.start();
		
		thread3.join();
		thread4.join();
		thread5.join();
		
		System.out.println("Querying a view: ");
		System.out.println("Result of load is " + Viewer.loadQuery(ddoc_name, view_name, serverAddr, port));
		
		System.exit(0);
	}
}
