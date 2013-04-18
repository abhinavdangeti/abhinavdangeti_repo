import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.SerializingTranscoder;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

public class Escalated {
    private static String[] _serverAddrs = {"10.x.x.xxx", "10.x.x.xxx", "10.x.x.xxx"};
    private static int _port = 8091;
    private static String[] buckets = {"default", "memcached_bucket"};
    private static int _expiration_time = 3600;
    private static int _item_size = 2048;
    private static int _count = 20000;
    private static int _final = 10000000;
    private static String _prefix = "key";
    private static long _RUNTIME_ = 172800000; //48 hours in milliseconds
    static SerializingTranscoder _t_;
    
    static final CouchbaseClient connect(String _bucketName, String _bucketPwd) throws URISyntaxException, IOException{
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create(String.format("http://" + _serverAddrs[0] + ":" + Integer.toString(_port) + "/pools")));
        _t_ = new SerializingTranscoder();
        _t_.setCompressionThreshold(Integer.MAX_VALUE);
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        cfb.setTranscoder(_t_);
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
        //final CouchbaseClient mclient = connect(buckets[1], "");
        
        Runnable _memop_ = new Runnable() {
            public void run() {
                try {
			getter(dclient);
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
        _for_default_.start();					//Running ops on default
        
        Thread _for_memcachedbucket_ = new Thread(_memop_);
        _for_memcachedbucket_.start();			//Running gets on default
        
        _for_default_.join();
        _for_memcachedbucket_.interrupt();
        dclient.shutdown();
        //mclient.shutdown();
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
		setOp = client.set(key, _expiration_time, value.toString(), _t_);
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
            while (_curr_calc("curr_items", client) >= _count) {        // Set only if item count is less than 20000
                Thread.sleep(10000);
            }
	    OperationFuture<Boolean> setOp;
	    String key = String.format("%s%d", _prefix, i);
   	    setOp = client.set(key, _expiration_time, value.toString(), _t_);
	    i++;
	    if (setOp.get().booleanValue() == false){
		continue;
	    }
	    endTime = System.currentTimeMillis();
	}
		
    }
    
    private static int _curr_calc(String parameter, CouchbaseClient client) throws UnknownHostException {
    	int _ep_curr_items = 0;
	Map<SocketAddress, Map<String, String>> map = client.getStats();
	Iterator<SocketAddress> iterator = map.keySet().iterator();
        
	Map<String, Integer> hm = new HashMap<String, Integer>();
	while (iterator.hasNext()){
            Object key = iterator.next();
			
	    if (hm.containsKey(key.toString())) {
                System.out.println("yes");
                if (hm.size() == _serverAddrs.length)
                    break;
                continue;
	    }
            Map<String, String> map1 = map.get(key);
            Iterator<String> tt = map1.keySet().iterator();
            //System.out.println(key.toString());
            while (tt.hasNext()) {
                if (hm.containsKey(key)) {
                    break;
                }
                String val1 = tt.next();
                if ((val1.toString().equals(parameter))) {
                    String val2 = map1.get(val1);
                    //System.out.println(val1.toString() + "  " + val2.toString());
                    hm.put(key.toString(), Integer.parseInt(val2.toString()));
                }
            }
	}
		
	Iterator<String> it = hm.keySet().iterator();
        while (it.hasNext()) {
            _ep_curr_items += hm.get(it.next().toString());
        }
	return _ep_curr_items;
    }
    
    @SuppressWarnings("unused")
	private static void getter(CouchbaseClient client) throws InterruptedException, ExecutionException, URISyntaxException, IOException {
	    while (true) {
		Thread.sleep(5000);
		for (int i=0; i<_final; i++) {
		    Object getObject = null;
		    try {
			getObject = client.get(String.format("%s%d", _prefix, i), _t_);
		    } catch (Exception e) {
			//Get didn't fetch
		    }
		}
	    }
	}
    
}
