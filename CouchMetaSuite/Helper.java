import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.codehaus.jettison.json.JSONException;

import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.CouchbaseMetaClient;


public class Helper {

    private static String[] _sourceNodes = {}; 
    private static String _sourcePort = "";
    private static String[] _destinationNodes = {};
    private static String _destinationPort = "";	
    private static String _bucketName = "default";
    private static String _bucketPasswd = "";

    public static void main(String args[]) throws MalformedURLException, IOException, JSONException {

	Stronghold sh = new Stronghold();

	try {
	    File file = new File("test.properties");
	    FileInputStream fileInput = new FileInputStream(file);
	    Properties properties = new Properties();
	    properties.load(fileInput);
	    fileInput.close();

	    parse_input(properties, sh);
	} catch (Exception e) {
	    e.printStackTrace();
	}
	/*
	// Creating a bucket and stuff at source and destination
	ClusterSetup._setupCluster(_sourceNodes, _sourcePort);
	ClusterSetup._createBucket(sh, _sourceNodes[0], _sourcePort, _bucketName, _bucketPasswd);

	ClusterSetup._setupCluster(_destinationNodes, _destinationPort);
	ClusterSetup._createBucket(sh, _destinationNodes[0], _destinationPort, _bucketName, _bucketPasswd);

	ClusterSetup._setupReplication(_sourceNodes[0], _destinationNodes[0], _sourcePort, _bucketName);
	 */		
	// Connection to source's and destination's bucket
	final CouchbaseMetaClient source_client = connect(_sourceNodes[0], _sourcePort);
	final CouchbaseMetaClient destination_client = connect(_destinationNodes[0], _destinationPort);

	// Operation that setrm's on source, and setwithMeta's on destination with the meta from setrm
	Setrunner.sets(sh, source_client, destination_client);

	// Operation that delrm's on source, and delwithMeta's on destination with the meta from delrm

	// Operation that addrm's on source, and addwithMeta's on destination with the meta from addrm
	Addrunner.adds(sh, source_client, destination_client);

	// VERIFICATION

    }

    private static CouchbaseMetaClient connect(String _addr, String _port) {
	/*
	 * CouchbaseMetaClient Connection to bucket at the specified server
	 */
	List<URI> uris = new LinkedList<URI>();
	uris.add(URI.create(String.format("http://" + _addr + ":" + _port + "/pools")));
	CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
	try {
	    return new CouchbaseMetaClient(cfb.buildCouchbaseConnection(uris, _bucketName, _bucketPasswd));
	} catch (Exception e) {
	    System.err.println("Error connecting to Couchbase: "
		    + e.getMessage());
	    System.exit(0);
	}
	return null;
    }

    private static void parse_input(Properties properties, Stronghold sh) {
	/*
	 * Read test variables from test.properties file
	 */

	Enumeration<Object> enuKeys = properties.keys();
	while(enuKeys.hasMoreElements()){
	    String key = (String) enuKeys.nextElement();
	    if (key.equals("source"))
		_sourceNodes = properties.getProperty(key).split(",");
	    if (key.equals("source-port"))
		_sourcePort = properties.getProperty(key);
	    if (key.equals("destination"))
		_destinationNodes = properties.getProperty(key).split(",");
	    if (key.equals("destination-port"))
		_destinationPort = properties.getProperty(key);
	    if (key.equals("bucket-name"))
		_bucketName = properties.getProperty(key);
	    if (key.equals("bucket-password"))
		_bucketPasswd = properties.getProperty(key);

	    if (key.equals("bucket-memQuota"))
		sh.setMemquota(Integer.parseInt(properties.getProperty(key)));
	    if (key.equals("json"))
		sh.setJson(Boolean.parseBoolean(properties.getProperty(key)));
	    if (key.equals("item-count"))
		sh.setItemcount(Integer.parseInt(properties.getProperty(key)));
	    if (key.equals("item-size"))
		sh.setSize(Integer.parseInt(properties.getProperty(key)));
	    if (key.equals("prefix"))
		sh.setPrefix(properties.getProperty(key));
	    if (key.equals("exp-ratio"))
		sh.setExpRatio(Float.parseFloat(properties.getProperty(key)));
	    if (key.equals("expiration-time"))
		sh.setExpiration(Integer.parseInt(properties.getProperty(key)));
	    if (key.equals("del-ratio"))
		sh.setDelRatio(Float.parseFloat(properties.getProperty(key)));
	    if (key.equals("add-count"))
		sh.setAddCount(Integer.parseInt(properties.getProperty(key)));
	}

    }
}
