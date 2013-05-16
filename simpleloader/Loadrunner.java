import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

public class Loadrunner {

    private static String _bucketName = "default";
    private static String _bucketPwd = "";
    private static String _serverAddr = "127.0.0.1";
    private static String _port = "8091";
    private static int _itemCount = 0;
    private static int _itemSize = 0;							//In bytes
    private static int _addMore = 0;							//No. of items more to add (through sets) to the created list
    private static boolean _json = false;
    private static String _prefix = "";
    private static double _delRatio = 0.0;

    public static void main(String args[]) throws URISyntaxException, IOException{

	try {
	    File file = new File("test.properties");
	    FileInputStream fileInput = new FileInputStream(file);
	    Properties properties = new Properties();
	    properties.load(fileInput);
	    fileInput.close();

	    parse_input(properties);

	} catch (Exception e) {
	    e.printStackTrace();
	}

	final CouchbaseClient client = connect();

	//SETs thread
	Runnable _setRun = new Runnable() {
	    public void run() {
		System.out.println("Sets' thread starting up .. (" + _itemCount + " items)");
		try {
		    Creater.create_items(_itemCount, _itemSize, _json, client, _prefix);
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	};

	//GETs thread
	Runnable _getRun = new Runnable() {
	    public void run() {
		System.out.println("Gets' thread starting up ..");
		try {
		    Getter.get_em_all(_itemCount, client, _prefix);
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	};

	//DELETEs thread
	Runnable _delRun = new Runnable() {
	    public void run() {
		int del_count = (int)((double)_itemCount * _delRatio);
		System.out.println("Deletes' thread starting up .. ");
		try {
		    Deleter.del_items(del_count, client, _prefix);
		} catch (Exception e) {
		    e.printStackTrace();
		}
	    }
	};

	//GETs
	Thread _getThread = new Thread(_getRun);
	_getThread.start();

	//SETs
	Thread _setThread = new Thread(_setRun);
	_setThread.start();

	//Wait for sets thread to complete
	try {
	    _setThread.join();
	    System.out.println("Sets' thread - Done");
	} catch (Exception e) {
	    e.printStackTrace();
	}

	//Sleep 10s
	try {
	    Thread.sleep(10000);
	} catch (Exception e) {
	    //do nothing
	}

	//DELETEs
	Thread _delThread = new Thread(_delRun);
	_delThread.start();

	//Wait for deletes thread to complete
	try {
	    _delThread.join();
	    System.out.println("Deletes' thread - Done");
	} catch (Exception e) {
	    e.printStackTrace();
	}

	//Sleep 10s
	try {
	    Thread.sleep(20000);
	} catch (Exception e) {
	    //do nothing
	}

	_getThread.interrupt();
	System.out.println("Gets' thread - Killed");
	client.shutdown();
	System.out.println("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ");
	System.out.println("DONE");
	System.exit(0);

    }

    @SuppressWarnings("rawtypes")
	public static void parse_input(Properties properties) {
	    Enumeration enuKeys = properties.keys();
	    while(enuKeys.hasMoreElements()){
		String key = (String) enuKeys.nextElement();
		if (key.equals("server"))
		    _serverAddr = properties.getProperty(key);
		if (key.equals("port"))
		    _port = properties.getProperty(key);
		if (key.equals("bucket-name"))
		    _bucketName = properties.getProperty(key);
		if (key.equals("bucket-password"))
		    _bucketPwd = properties.getProperty(key);
		if (key.equals("json"))
		    _json = Boolean.parseBoolean(properties.getProperty(key));
		if (key.equals("item-count"))
		    _itemCount = Integer.parseInt(properties.getProperty(key));
		if (key.equals("item-size"))
		    _itemSize = Integer.parseInt(properties.getProperty(key));
		if (key.equals("prefix"))
		    _prefix = properties.getProperty(key);
		if (key.equals("del-ratio"))
		    _delRatio = Float.parseFloat(properties.getProperty(key));
	    }
	}

    static final CouchbaseClient connect() throws URISyntaxException, IOException{
	List<URI> uris = new LinkedList<URI>();
	uris.add(URI.create(String.format("http://" + _serverAddr + ":" + _port + "/pools")));
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

}
