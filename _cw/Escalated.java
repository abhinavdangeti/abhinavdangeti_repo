import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

public class Escalated {
    private static String[] _serverAddrs = {"10.x.x.xxx", "10.x.x.xxx", "10.x.x.xxx"};
    private static int _port = 8091;
	private static String[] buckets = {"default", "memcached_bucket"};
    private static int _expiration_time = 3600;
    private static int _item_size = 1024;
    private static int _count = 20000;
//    private static int _final = 10000000;
    private static String _prefix = "key";
    private static long _RUNTIME_ = 172800000; //48 hours in milliseconds
    
    static final CouchbaseClient connect(String _bucketName, String _bucketPwd) throws URISyntaxException, IOException{
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create(String.format("http://" + _serverAddrs[0] + ":" + Integer.toString(_port) + "/pools")));
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
        
        for (String s : buckets) {
            CouchbaseClient client = connect(s, "");
            load_initial(client);
            client.shutdown();
        }
        
        System.out.println("Completed stage 1: initial load on all buckets ..");
        
        final CouchbaseClient dclient = connect(buckets[0], "");
        final CouchbaseClient mclient = connect(buckets[1], "");
        
        Runnable _memop_ = new Runnable() {
            public void run() {
                try {
					getter(mclient);
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
        
        Runnable _defop_ = new Runnable() {
            public void run() {
                try {
					load_post(dclient);
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        };
        
        Thread _for_default_ = new Thread(_defop_);
        _for_default_.start();
        
        Thread _for_memcachedbucket_ = new Thread(_memop_);
        _for_memcachedbucket_.start();
        
        _for_default_.join();
        _for_memcachedbucket_.interrupt();
        dclient.shutdown();
        mclient.shutdown();
        System.exit(0);
        
    }
    
    private static void load_initial(CouchbaseClient client) throws InterruptedException, ExecutionException {
		StringBuffer value = new StringBuffer();
		String CHAR_LIST = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        while (value.length() < _item_size) {
            value.append(CHAR_LIST);
        }
		for (int i=0; i<_count; i++){
			OperationFuture<Boolean> setOp;
			String key = String.format("%s%d", _prefix, i);
			setOp = client.set(key, _expiration_time, value.toString());
			if (setOp.get().booleanValue() == false){
				continue;
			}
		}		
	}
    
    private static void load_post(CouchbaseClient client) throws InterruptedException, ExecutionException, UnknownHostException {
		StringBuffer value = new StringBuffer();
		String CHAR_LIST = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        while (value.length() < _item_size) {
            value.append(CHAR_LIST);
        }
		//for (int i=_count; i<_final; i++){
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        int i = 0;
		while ((endTime - startTime) < _RUNTIME_){
            while (_curr_calc(client) >= _count) {        // Set only if item count is less than 20000
                Thread.sleep(10000);
            }
			OperationFuture<Boolean> setOp;
			String key = String.format("%s%d", _prefix, i);
			setOp = client.set(key, _expiration_time, value.toString());
			i++;
			if (setOp.get().booleanValue() == false){
				continue;
			}
			endTime = System.currentTimeMillis();
		}
		
	}
 
    private static int _curr_calc(CouchbaseClient client) throws UnknownHostException {
    	int _ep_curr_items = 0;
		Map<SocketAddress, Map<String, String>> map = client.getStats();
		for (String server : _serverAddrs) {
			InetAddress _inetAddress = InetAddress.getByName(server);
	      	SocketAddress socketAddress = new InetSocketAddress(_inetAddress, _port);
	      	_ep_curr_items += Integer.parseInt(map.get(socketAddress).get("ep_curr_items"));
		}
		return _ep_curr_items;
    }

    private static void getter(CouchbaseClient client) throws InterruptedException, ExecutionException, URISyntaxException, IOException {
		while (true) {
			for (int i=0; i<_count; i++) {
				Object getObject = null;
				try {
					getObject = client.get(String.format("%s%d", _prefix, i));
				} catch (Exception e) {
					//Get didn't fetch
				}
				if (getObject == null)
					break;
			}
		}
	}
    
}
