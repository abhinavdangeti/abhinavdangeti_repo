import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

public class Mixer {
	private static String _serverAddr = "10.3.2.49";
	private static int _port = 8091;
	private static int _initial_load = 1000000;
	private static int _post_load = 800000;
	private static int _x_ = 100000;
	private static String _prefix = "key";
	private static int _item_size = 512;
	private static int _expiration_time = 1800;
	private static int del_multiplier = 3;
	
	static final CouchbaseClient connect(String _bucketName, String _bucketPwd) throws URISyntaxException, IOException{
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create(String.format("http://" + _serverAddr + ":" + Integer.toString(_port) + "/pools")));
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        try {
	     	return new CouchbaseClient(cfb.buildCouchbaseConnection(uris, _bucketName, _bucketPwd));
        } catch (Exception e) {
	     	System.err.println("Error connecting to Couchbase: "
                               + e.getMessage());
	      	System.exit(0);
        }
        return null;	    
    }
	
	public static void main(String args[]) throws URISyntaxException, IOException, InterruptedException, ExecutionException{
		
		ArrayList<String> couchbase_buckets = new ArrayList<String>();
		couchbase_buckets.add("default");
		couchbase_buckets.add("standard_bucket0");
		couchbase_buckets.add("standard_bucket1");
		couchbase_buckets.add("standard_bucket2");
		
		String memcached_bucket = "memcached_bucket0";
		
		for (String s : couchbase_buckets) {
			CouchbaseClient client = connect(s, "");
			load_initial(_initial_load, _item_size, client);
			client.shutdown();
		}
        
		CouchbaseClient mclient = connect(memcached_bucket, "");
		load_initial(_initial_load, _item_size, mclient);
		mclient.shutdown();
        
		System.out.println("--< COMPLETED STAGE 1 >--");
        
		System.out.println("Enter something to start work on default bucket");
		Scanner sc = new Scanner(System.in);
		@SuppressWarnings("unused")
		String _ok_1 = sc.nextLine();
        
		Runnable _bg_ = new Runnable() {
			public void run() {
				try {
					theother();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
        
		/*
		 * CHOOSING DEFAULT BUCKET
		 */
        
		final CouchbaseClient dclient = connect("default", "");
        
		Runnable _exp_ = new Runnable() {
			public void run() {
				try {
					expire_post(_initial_load + _post_load - (6*_x_), _initial_load + _post_load - (5*_x_), _expiration_time, dclient);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		
		Runnable _del_ = new Runnable() {
			public void run() {
				try {
					delete_post(dclient);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		
		Runnable _upd_ = new Runnable() {
			public void run() {
				try {
					update_post(dclient);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		
		//Load till higher watermark
		load_post(_initial_load, _initial_load + _post_load, _item_size, dclient);
		Thread.sleep(5000);
		//background load on standard_bucket1,2
		Thread bg_setget_er = new Thread(_bg_);
		bg_setget_er.start();
		
		Thread expirer = new Thread(_exp_);
		Thread deleter = new Thread(_del_);
		Thread updater = new Thread(_upd_);
		
		for (int k=0; k<del_multiplier; k++) {
			
			System.out.println("Starting expirer thread ..");
			expirer.start();
			System.out.println("Starting deleter thread ..");
			deleter.start();
			System.out.println("Starting updater thread ..");
			updater.start();
			updater.join();
			System.out.println("Updater terminated!");
			deleter.join();
			System.out.println("Deleter terminated!");
			expirer.join();
			System.out.println("Expirer terminated!");
            
			System.out.println("Initial expiring of 100K items completed already, now expiring 90% of initial load ..");
			
			expire_post(0, _initial_load, _expiration_time, dclient);
			
			System.out.println("\n --< WAIT FOR ABOUT HALF AN HOUR BEFORE MANUALLY RUNNING EXPIRY PAGER ON DEFAULT and then hit ENTER (RUN " + k+1 + ") >-- \n");
			sc = new Scanner(System.in);
			@SuppressWarnings("unused")
			String _ok_2 = sc.nextLine();
			
			Thread.sleep(3000);
			load_initial(_initial_load, _item_size, dclient);
			Thread.sleep(10000);
		}
		//expire, delete, update items
        
		System.out.println("--< COMPLETED STAGE 2 >--");
        
		System.out.println("Press enter to quit the _bg_ thread and terminate ..");
		sc = new Scanner(System.in);
		@SuppressWarnings("unused")
		String _ok_3 = sc.nextLine();
        
		bg_setget_er.interrupt();
		dclient.shutdown();
		System.exit(0);
	}
    
	protected static void theother() throws InterruptedException, ExecutionException, URISyntaxException, IOException {
		CouchbaseClient client1 = connect("standard_bucket2", "");
		StringBuffer value = new StringBuffer();
		String CHAR_LIST = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        while (value.length() < _item_size) {
            value.append(CHAR_LIST);
        }
        //        List<OperationFuture<Boolean>> sets = new LinkedList<OperationFuture<Boolean>>();
        for (int i=3 * _initial_load / 4; i<5 * _initial_load / 4; i++){
        	String key = String.format("%s%d", _prefix, i);
        	OperationFuture<Boolean> setOp = client1.set(key, 0, value.toString());
            //        	sets.add(setOp);
        	if (setOp.get().booleanValue() == false){
				continue;
			}
        }
        //		while (!sets.isEmpty()) {
        //			if (sets.get(0).get().booleanValue() == false){
        //				System.err.println("Update failed: "/* + setOp.getStatus().getMessage()*/);
        //				continue;
        //			} else 
        //				sets.remove(0);
        //		}
		client1.shutdown();
		
		CouchbaseClient client2 = connect("standard_bucket1", "");
		while (true) {
			for (int i=0; i<_initial_load; i++) {
				Object getObject = null;
				try {
					getObject = client2.get(String.format("Key-%d", i));
				} catch (Exception e) {
					//Get didn't fetch
				}
				if (getObject == null)
					break;
			}
		}
	}
    
	protected static void update_post(CouchbaseClient dclient) throws InterruptedException, ExecutionException {
		StringBuffer value = new StringBuffer();
		String CHAR_LIST = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        while (value.length() < _item_size) {
            value.append(CHAR_LIST);
        }
        //        List<OperationFuture<Boolean>> updates = new LinkedList<OperationFuture<Boolean>>();
		for (int i=_initial_load + _post_load - _x_; i<_initial_load + _post_load; i++){
			String key = String.format("%s%d", _prefix, i);
			OperationFuture<Boolean> repOp = dclient.replace(key, 0, value.toString());
            //			updates.add(repOp);
			if (repOp.get().booleanValue() == false){
				continue;
			}
		}
        //		while (!updates.isEmpty()) {
        //			if (updates.get(0).get().booleanValue() == false){
        //				System.err.println("Update failed: "/* + repOp.getStatus().getMessage()*/);
        //				continue;
        //			} else
        //				updates.remove(0);
        //		}
	}
    
	protected static void delete_post(CouchbaseClient dclient) throws InterruptedException, ExecutionException {
        //		List<OperationFuture<Boolean>> deletes = new LinkedList<OperationFuture<Boolean>>();
		for (int i=_initial_load + _post_load - (5*_x_); i<_initial_load + _post_load - _x_; i++){
			OperationFuture<Boolean> delOp = dclient.delete(String.format("%s%d", _prefix, i));
            //			deletes.add(delOp);
			if (delOp.get().booleanValue() == true){
				continue;
			}
		}
        //		while (!deletes.isEmpty()) {
        //			if (deletes.get(0).get().booleanValue() == true){
        //				System.err.println("Delete failed: "/* + delOp.getStatus().getMessage()*/);
        //				continue;
        //			} else
        //				deletes.remove(0);
        //		}
	}
    
	protected static void expire_post(int start, int end, int expiration, CouchbaseClient dclient) throws InterruptedException, ExecutionException {
		StringBuffer value = new StringBuffer();
		String CHAR_LIST = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        while (value.length() < _item_size) {
            value.append(CHAR_LIST);
        }
        //        List<OperationFuture<Boolean>> expires = new LinkedList<OperationFuture<Boolean>>();
		for (int i=start; i<end; i++){
			String key = String.format("%s%d", _prefix, i);
			OperationFuture<Boolean> expOp = dclient.replace(key, expiration, value.toString());
            //			expires.add(expOp);
			if (expOp.get().booleanValue() == false){
				continue;
			}
		}
        //		while (!expires.isEmpty()) {
        //			if (expires.get(0).get().booleanValue() == false){
        //				System.err.println("Update failed: "/* + expOp.getStatus().getMessage()*/);
        //				continue;
        //			} else
        //				expires.remove(0);
        //		}
		
	}
    
	private static void load_post(int start, int end, int _itemSize, CouchbaseClient client) throws InterruptedException, ExecutionException {
		StringBuffer value = new StringBuffer();
		String CHAR_LIST = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        while (value.length() < _itemSize) {
            value.append(CHAR_LIST);
        }
        //        List<OperationFuture<Boolean>> creates = new LinkedList<OperationFuture<Boolean>>();
		for (int i=start; i<end; i++){
			OperationFuture<Boolean> setOp;
			String key = String.format("%s%d", _prefix, i);
			setOp = client.set(key, 0, value.toString());
			if (setOp.get().booleanValue() == false){
				continue;
			}
            //			creates.add(setOp);
		}
        //		while (!creates.isEmpty()) {
        //			if (creates.get(0).get().booleanValue() == false){
        //				System.err.println("Set failed: "/* + setOp.getStatus().getMessage()*/);
        //				continue;
        //			} else
        //				creates.remove(0);
        //		}
		
	}
    
	private static void load_initial(int _itemCount, int _itemSize, CouchbaseClient client) throws InterruptedException, ExecutionException {
		StringBuffer value = new StringBuffer();
		String CHAR_LIST = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        while (value.length() < _itemSize) {
            value.append(CHAR_LIST);
        }
        //        List<OperationFuture<Boolean>> creates = new LinkedList<OperationFuture<Boolean>>();
		for (int i=0; i<_itemCount; i++){
			OperationFuture<Boolean> setOp;
			String key = String.format("%s%d", _prefix, i);
			setOp = client.set(key, 0, value.toString());
			if (setOp.get().booleanValue() == false){
				continue;
			}
            //			creates.add(setOp);
		}
        //		while (!creates.isEmpty()) {
        //			if (creates.get(0).get().booleanValue() == false){
        //				System.err.println("Set failed: "/* + setOp.getStatus().getMessage()*/);
        //				continue;
        //			} else
        //				creates.remove(0);
        //		}
		
	}
}
