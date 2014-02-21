import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
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
    private static Boolean writetofile = false;

    public static HashMap<String, String> n1 = new HashMap<String,String>();
    public static HashMap<String, String> n2 = new HashMap<String,String>();

    public static void main(String args[]) throws URISyntaxException, IOException, InterruptedException, ExecutionException {
        CouchbaseMetaClient s_client = connect(_node1, _port1);
        CouchbaseMetaClient d_client = connect(_node2, _port2);

        I_terator(s_client, d_client);

        for (Map.Entry<String, String> htEntries : n1.entrySet()) {
            if(!(n2.containsKey(htEntries.getKey()) && n2.get(htEntries.getKey()).equals(htEntries.getValue()))){
                System.out.println("\tKey: " + htEntries.getKey() + " Value: " + htEntries.getValue() + " :: mismatch in n1, n2\n");
            }
        }

        System.exit(0);

    }

    @SuppressWarnings("rawtypes")
    private static void I_terator(CouchbaseMetaClient client1, CouchbaseMetaClient client2) throws IOException {
        for (int i = 0; i < _itemCnt; i++) {
            OperationFuture<MetaData> retm = null;
            String key = String.format("%s%d", _prefix, i);
            try {
                retm = client1.getReturnMeta(key);
                n1.put(key, retm.get().toString());
                retm = client2.getReturnMeta(key);
                n2.put(key, retm.get().toString());
            } catch (Exception e) {
                //Do nothing for now
            }
        }

        if (writetofile) {
            System.out.println("Writing data to files source_data_log.txt and destination_data_log.txt");
            File file1 = new File("source_data_log.txt");
            File file2 = new File("destination_data_log.txt");
            if (file1.exists())
                file1.delete();
            if (file2.exists())
                file2.delete();
            file1.createNewFile();
            file2.createNewFile();
            FileWriter fw1 = new FileWriter(file1.getAbsolutePath());
            BufferedWriter bw1 = new BufferedWriter(fw1);
            FileWriter fw2 = new FileWriter(file2.getAbsolutePath());
            BufferedWriter bw2 = new BufferedWriter(fw2);
            Iterator it1 = n1.entrySet().iterator();
            Iterator it2 = n2.entrySet().iterator();
            while(it1.hasNext()) {
                Map.Entry p1 = (Map.Entry) it1.next();
                String key = (String) p1.getKey();
                String val = (String) p1.getValue();
                //System.out.println(key + " -- " + val);
                bw1.write(key + " -- " + val + "\n");
            }
            while (it2.hasNext()) {
                Map.Entry p2 = (Map.Entry) it2.next();
                String key = (String) p2.getKey();
                String val = (String) p2.getValue();
                //System.out.println(key + " -- " + val);
                bw2.write(key + " -- " + val + "\n");
            }
            bw1.close();
            bw2.close();
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
