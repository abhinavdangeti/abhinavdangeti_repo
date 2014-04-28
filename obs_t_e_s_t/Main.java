import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
//import java.util.Random;


import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Main {
    private static String _serverAddr = "127.0.0.1";
    private static int _port = 9000;
    private static String bucket = "default";
    private static int num_samples = 100;
    private static boolean doPrintEntries = true;

    static final CouchbaseClient connect() throws URISyntaxException, IOException{
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create(String.format("http://" + _serverAddr + ":" + Integer.toString(_port) + "/pools")));
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        try {
            return new CouchbaseClient(cfb.buildCouchbaseConnection(uris, bucket, ""));
        } catch (Exception e) {
            System.err.println("Error connecting to Couchbase: "
                    + e.getMessage());
            System.exit(0);
        }
        return null;
    }

    public static void main(String args[])
        throws URISyntaxException, IOException, InterruptedException, ExecutionException, JSONException {
        CouchbaseClient client = connect();
        String key = "test_key";
        //Random randgen = new Random();
        ArrayList<Double> arr = new ArrayList<Double>();
        System.out.println("- + - + - + - + - + - + - + - + - + - + -");
        for (int i=0; i<num_samples; i++) {
            //int sample = randgen.nextInt(1000)%30 + 3;
            //System.out.println(" Sleep for " + sample + "s.");
            //Thread.sleep(sample * 1000);
            JSONObject value = new JSONObject("{\"value\":\"" + i + "\"}");
            long preOBS = System.nanoTime();
            OperationFuture<Boolean> setOp = client.set(key + i, value.toString(), ReplicateTo.ONE);
            long postOBS = System.nanoTime();
            if (setOp.get().booleanValue() == false){
                System.err.println("Set failed: " + setOp.getStatus().getMessage());
                //System.exit(1);
            } else {
                double tot_time = (double)(postOBS - preOBS) / 1000000.0;
                arr.add(tot_time);
            }
            //System.out.println("Time for Observe To Replica: " + tot_time + " ms.");
        }
        if (arr.size() > 0) {
            double sum = 0;
            Object[] elements = arr.toArray();
            for (int j=0; j<arr.size(); j++) {
                sum = sum + Double.parseDouble(elements[j].toString());
            }
            System.out.println("Mean time:\n " + sum/arr.size() + " ms.");
            if (doPrintEntries) {
                System.out.println("- - - - - - - - - - - - - - - - - - - - -");
                for (int k=0; k<arr.size(); k++) {
                    System.out.print(elements[k].toString() + "  ");
                }
                System.out.println();
            }
        } else {
            System.out.println("-- NO RESULTS --");
        }
        System.out.println("- + - + - + - + - + - + - + - + - + - + -");
        client.shutdown();
        System.exit(0);
    }

}

