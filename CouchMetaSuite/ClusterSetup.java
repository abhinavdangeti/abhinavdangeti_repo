import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.couchbase.client.ClusterManager;

public class ClusterSetup {

    /*
     * Add nodes and rebalance
     */
    public static void _setupCluster (String[] _servers, String _port) throws MalformedURLException, IOException, JSONException {
	String _command1 = read_from_json("addNode", _servers[0], _port);
	for (int i=1;i<_servers.length;i++) {
	    String curlPOST = "curl -X POST -v -u Administrator:password -d \"hostname="
		+ _servers[i] + "\" -d \"user=Administrator\" -d \"password=password\" http://"
		+ _servers[0] + ":" + _port + "/" + _command1;
	    Runtime.getRuntime().exec(curlPOST);
	}
	String serverlist = "ns_1@" + _servers[0];
	for (int i=1;i<_servers.length;i++) {
	    serverlist += ",ns_1@" + _servers[i];
	}
	String _command2 = read_from_json("rebalance", _servers[0], _port);
	String curlPOST = "curl -X POST -v -u Administrator:password -d \"knownNodes="
	    + serverlist + "\" http://" + _servers[0] + ":" + _port + "/" + _command2;
	Runtime.getRuntime().exec(curlPOST);
    }

    /*
     * Create bucket over the specified cluster
     */
    public static void _createBucket (Stronghold sh, String _server, String _port, String _bName, String _bPasswd) {
	List<URI> uris = new LinkedList<URI>();
	uris.add(URI.create(String.format("http://" + _server + ":" + _port + "/pools")));
	ClusterManager cm = new ClusterManager(uris, "Administrator", "password");
	if (_bName.equals("default")) {
	    cm.createDefaultBucket(null, sh.getMemquota(), 0, true);		//replica count at zero, and flushenabled to true
	} else {
	    cm.createNamedBucket(null, _bName, sh.getMemquota(), 0, _bPasswd, true);	//sasl bucket
	}
	// TODO: Add option to create standard bucket on different port
    }

    /*
     * Set up replication from source cluster to destination cluster
     */
    public static void _setupReplication (String _source, String _destination, String _port, String _bPasswd) {

    }

    private static String read_from_json (String oper, String _server, String _port) throws MalformedURLException, IOException, JSONException {
	String thePOST = null;
	if (oper.equals("addNode")) {
	    InputStream is = new URL("http://" + _server + ":" + _port + "/pools/default/").openStream();
	    try {
		BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
		StringBuilder sb = new StringBuilder();
		int copy;
		while ((copy = reader.read()) != -1) {
		    sb.append((char) copy);
		}
		JSONObject json = new JSONObject(sb.toString());
		JSONObject nest1 = (JSONObject) json.get("controllers");
		thePOST = nest1.get("addNode").toString();
	    } finally {
		is.close();
	    }
	} else if (oper.equals("rebalance")) {
	    InputStream is = new URL("http://" + _server + ":" + _port + "/pools/default/").openStream();
	    try {
		BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
		StringBuilder sb = new StringBuilder();
		int copy;
		while ((copy = reader.read()) != -1) {
		    sb.append((char) copy);
		}
		JSONObject json = new JSONObject(sb.toString());
		JSONObject nest1 = (JSONObject) json.get("controllers");
		thePOST = nest1.get("rebalance").toString();
	    } finally {
		is.close();
	    }
	} else if (oper.equals("startReplication")) {

	}
	return thePOST;
    }

}
