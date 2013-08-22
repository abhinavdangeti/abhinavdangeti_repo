import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.Random;

import net.spy.memcached.ReplicateTo;
import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;


public class Main {
    private static String _serverAddr = "10.5.2.54";
    private static int _port = 8091;
    private static String bucket = "default";
    private static int num_samples = 5;

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

    public static void main(String args[]) throws URISyntaxException, IOException, InterruptedException, ExecutionException {
        CouchbaseClient client = connect();
        String key = "test_key";
        String value = "test_value";
        double sum = 0;
        Random randgen = new Random();
        for (int i=0; i<num_samples; i++) {
            long preOBS = System.nanoTime();
            OperationFuture<Boolean> setOp = client.set(key + i, value + i, ReplicateTo.ONE);
            long postOBS = System.nanoTime();
            if (setOp.get().booleanValue() == false){
                System.err.println("Set failed: " + setOp.getStatus().getMessage());
                System.exit(1);
            }
            double tot_time = (double)(postOBS - preOBS) / 1000000.0;
            System.out.println("Time for Observe To Replica: " + tot_time + " ms.");
            sum += tot_time;
            int sample = randgen.nextInt(1000)%15 + 3;
            System.out.println(" Sleep for " + sample + "s.");
            Thread.sleep(sample * 1000);
        }
        System.out.println("Mean time:\n " + sum/num_samples + " ms.");
        client.shutdown();
        System.exit(0);
    }

}

