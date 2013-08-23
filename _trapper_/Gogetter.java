import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

public class Gogetter {
    private static String _serverAddr = "10.3.2.55";
    private static int _port = 8091;
    private static String[] buckets = {"default"};
    private static int _initial_load = 100000000;
    private static int _x_ = 100000;
    private static String _prefix = "key";
    private static int _item_size = 64;
    private static int _expiration_time = 900;
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

        final CouchbaseClient dclient = connect("default", "");


        Runnable _load_ = new Runnable() {
            public void run() {
                try {
                    load_initial(_initial_load, _item_size, dclient);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        Runnable _bg_ = new Runnable() {
            public void run() {
                try {
                    theother(dclient);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        //frontend load on default
        Thread load_star = new Thread(_load_);
        load_star.start();

        //background load on default
        Thread bg_getter = new Thread(_bg_);
        bg_getter.start();

        load_star.join();

        Scanner sc = new Scanner(System.in);
        System.out.println("\n Press ENTER to terminate .. \n");
        sc = new Scanner(System.in);
        @SuppressWarnings("unused")
            String _ok_ = sc.nextLine();

        bg_getter.interrupt();
        dclient.shutdown();
        System.exit(0);
    }

    protected static void theother(CouchbaseClient client) throws InterruptedException, ExecutionException, URISyntaxException, IOException {

        while (true) {
            for (int i=0; i<_initial_load; i++) {
                Object getObject = null;
                try {
                    getObject = client.get(String.format("Key-%d", i));
                } catch (Exception e) {
                    //Get didn't fetch
                }
                if (getObject == null)
                    break;
            }
        }
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
