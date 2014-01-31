import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.internal.OperationFuture;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.CouchbaseMetaClient;
import com.couchbase.client.MetaData;

public class Orchestrator {

    private static String _node1 = "127.0.0.1";
    private static int _port1 = 9000;
    private static String _node2 = "127.0.0.1";
    private static int _port2 = 9001;
    private static String _bkt = "default";
    private static String _pass = "";
    private static int _itemCnt = 10000;
    private static String _prefix = "";

    public static HashMap<String, String> n1 = new HashMap<String,String>();
    public static HashMap<String, String> n2 = new HashMap<String,String>();

    public static void main(String args[]) throws URISyntaxException, IOException, InterruptedException, ExecutionException {
        CouchbaseMetaClient s_client = connect(_node1, _port1);
        CouchbaseMetaClient d_client = connect(_node2, _port2);

        I_terator(s_client, d_client);

        for (Map.Entry<String, String> htEntries : n1.entrySet()) {
            if(!(n2.containsKey(htEntries.getKey()) && n2.get(htEntries.getKey()).equals(htEntries.getValue()))){
                System.out.println("\tKey: " + htEntries.getKey() + " Value: " + htEntries.getValue() + "mismatch in n1, n2\n");
            }
        }

        System.exit(0);

    }

    private static void I_terator(CouchbaseMetaClient client1, CouchbaseMetaClient client2) {
        for (int i = 0; i < _itemCnt; i++) {
            OperationFuture<MetaData> retm = null;
            String key = String.format("%s%d", _prefix, i);
            try {
                retm = client1.getReturnMeta(key);
                n1.put(key, retm.get().toString());
                retm = client2.getReturnMeta(key);
                n2.put(key, retm.get().toString());
            } catch (Exception e) {

            }
        }
    }

    private static final CouchbaseMetaClient connect(String _serverAddr, int port) throws URISyntaxException, IOException{
        List<URI> uris = new LinkedList<URI>();
        uris.add(URI.create(String.format("http://" + _serverAddr + ":" + Integer.toString(port) + "/pools")));
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        try {
            return new CouchbaseMetaClient(cfb.buildCouchbaseConnection(uris, _bkt, _pass));
        } catch (Exception e) {
            System.err.println("Error connecting to Couchbase: " + e.getMessage());
            System.exit(0);
        }
        return null;
    }
}
