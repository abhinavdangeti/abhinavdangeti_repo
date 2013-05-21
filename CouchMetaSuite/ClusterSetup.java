import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.couchbase.client.ClusterManager;
import com.couchbase.client.clustermanager.BucketType;

public class ClusterSetup {

	/*
	 * Add nodes and rebalance
	 */
	public static void _setupCluster (String[] _servers, String _port) 
			throws MalformedURLException, IOException, JSONException, InterruptedException {
		String _command1 = read_from_json("addNode", _servers[0], _port);
		for (int i=1;i<_servers.length;i++) {
			String curlPOST = "curl -X POST -v -u Administrator:password -d \"hostname="
					+ _servers[i] + "\" -d \"user=Administrator\" -d \"password=password\" http://"
					+ _servers[0] + ":" + _port + "/" + _command1;
			System.out.println("- Running Command: " + curlPOST);
			Process p = Runtime.getRuntime().exec(curlPOST);
			p.waitFor();
		}
		
		String serverlist = "ns_1@" + _servers[0];
		for (int i=1;i<_servers.length;i++) {
			serverlist += ",ns_1@" + _servers[i];
		}
		String _command2 = read_from_json("rebalance", _servers[0], _port);
		String curlPOST = "curl -X POST -v -u Administrator:password -d \"knownNodes="
				+ serverlist + "\" http://" + _servers[0] + ":" + _port + "/" + _command2;
		System.out.println("- Running Command: " + curlPOST);
		Process p = Runtime.getRuntime().exec(curlPOST);
		p.waitFor();
	}

	/*
	 * Create bucket over the specified cluster
	 */
	public static void _createBucket (Stronghold sh, String _server, String _port, String _bName, String _bPasswd) {
		List<URI> uris = new LinkedList<URI>();
		uris.add(URI.create(String.format("http://" + _server + ":" + _port + "/pools")));
		ClusterManager cm = new ClusterManager(uris, "Administrator", "password");
		System.out.println("- Creating bucket " + _bName + " at server " + _server + ":" + _port);
		if (_bName.equals("default")) {
			cm.createDefaultBucket(BucketType.COUCHBASE, sh.getMemquota(), 0, true);		//replica count at zero, and flush-enabled to true
		} else {
			cm.createNamedBucket(BucketType.COUCHBASE, _bName, sh.getMemquota(), 0, _bPasswd, true);	//sasl bucket
		}
		// TODO: Add option to create standard bucket on different port
	}

	/*
	 * Create remote reference and set up replication from source cluster to destination cluster
	 */
	public static void _setupReplication (String _source, String _destination, String _port, String _bName) 
			throws MalformedURLException, IOException, JSONException, InterruptedException {
		String _command1 = read_from_json("remoteClusters", _source, _port);
		String curlPOST1 = "curl -X POST -v -u Administrator:password -d \"password=password\" " +
				"-d \"username=Administrator\" -d \"hostname=" 
				+ _destination + ":" + _port + "\" -d \"name=remote\" http://" + _source + ":" + _port + "/" + _command1;
		System.out.println("- Running Command: " + curlPOST1);
		Process p1 = Runtime.getRuntime().exec(curlPOST1);
		p1.waitFor();
		
		String _command2 = read_from_json("replication", _source, _port);
		String curlPOST2 = "curl -X POST -v -u Administrator:password -d \"password=password\" "
				+ "-d \"replicationType=continuous\" -d \"toBucket=" 
				+ _bName + "\" -d \"toCluster=remote\" -d \"fromBucket=" + _bName 
				+ "\" http://" + _source + ":" + _port + "/" + _command2;
		System.out.println("- Running Command: " + curlPOST2);
		Process p2 = Runtime.getRuntime().exec(curlPOST2);
		p2.waitFor();
	}

	public static void _waitforrebalance (String _server, String _port) 
			throws MalformedURLException, IOException, JSONException, InterruptedException {
		/*
		 * String _command = read_from_json("wait_for_rebalance", _server, _port);
		 * System.out.println("-" + _command); 
		 */
		Thread.sleep(30000);
	}
	
	private static String read_from_json (String oper, String _server, String _port) 
			throws MalformedURLException, IOException, JSONException, InterruptedException {
		String thePOST = null;
		if (oper.equals("addNode")) {
			InputStream is = new URL("http://Administrator:password@" + _server + ":" + _port + "/pools/default/").openStream();
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
				StringBuilder sb = new StringBuilder();
			    int copy;
			    while ((copy = reader.read()) != -1) {
			      sb.append((char) copy);
			    }
			    JSONObject json = new JSONObject(sb.toString());
			    JSONObject nest1 = (JSONObject) json.get("controllers");
			    JSONObject nest2 = (JSONObject) nest1.get("addNode");
			    thePOST = nest2.get("uri").toString();
			} finally {
				is.close();
			}
		} else if (oper.equals("rebalance")) {
			InputStream is = new URL("http://Administrator:password@" + _server + ":" + _port + "/pools/default/").openStream();
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
				StringBuilder sb = new StringBuilder();
			    int copy;
			    while ((copy = reader.read()) != -1) {
			      sb.append((char) copy);
			    }
			    JSONObject json = new JSONObject(sb.toString());
			    JSONObject nest1 = (JSONObject) json.get("controllers");
			    JSONObject nest2 = (JSONObject) nest1.get("rebalance");
			    thePOST = nest2.get("uri").toString();
			} finally {
				is.close();
			}
		} else if (oper.equals("remoteClusters")) {
			InputStream is = new URL("http://Administrator:password@" + _server + ":" + _port + "/pools/default/").openStream();
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
				StringBuilder sb = new StringBuilder();
			    int copy;
			    while ((copy = reader.read()) != -1) {
			      sb.append((char) copy);
			    }
			    JSONObject json = new JSONObject(sb.toString());
			    JSONObject nest1 = (JSONObject) json.get("remoteClusters");
			    thePOST = nest1.get("uri").toString();
			} finally {
				is.close();
			}
		} else if (oper.equals("replication")) {
			InputStream is = new URL("http://Administrator:password@" + _server + ":" + _port + "/pools/default/").openStream();
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
				StringBuilder sb = new StringBuilder();
			    int copy;
			    while ((copy = reader.read()) != -1) {
			      sb.append((char) copy);
			    }
			    JSONObject json = new JSONObject(sb.toString());
			    JSONObject nest1 = (JSONObject) json.get("controllers");
			    JSONObject nest2 = (JSONObject) nest1.get("replication");
			    thePOST = nest2.get("createURI").toString();
			} finally {
				is.close();
			}
		} else if (oper.equals("wait_for_rebalance")) {
			// TODO: Fix this
			InputStream is = new URL("http://Administrator:password@" + _server + ":" + _port + "/pools/default/tasks").openStream();
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
				StringBuilder sb = new StringBuilder();
			    int copy;
			    while ((copy = reader.read()) != -1) {
			      sb.append((char) copy);
			    }
			    JSONObject json = new JSONObject(sb.toString());
			    @SuppressWarnings("rawtypes")
				Iterator items = json.keys();
			    while (items.hasNext()) {
			    	JSONObject item = (JSONObject) items.next();
			    	if (item.get("type").toString().equals("rebalance")) {
			    		while (item.get("status").toString().equals("running")) {
			    			System.out.println("Wait for rebalance to complete .. sleep 20s");
			    			Thread.sleep(20000);
			    		}
			    		break;
			    	}
			    }
			    thePOST = "Rebalance completed";
			} finally {
				is.close();
			}
		}
		return thePOST;
	}

}
