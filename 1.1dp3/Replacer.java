import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import net.spy.memcached.PersistTo;
import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;


public class Replacer {

	/*
	 * Method that replaces x number of items after NUM_ITEMS.
	 * Enabling OBSERVE, makes the replacer check if every item replaced has persisted or not.
	 * Replaced items have key [Key-i] and corresponding json document [new-i].
	 */
	@SuppressWarnings("unused")
	public static double replace_items(int number_items, int item_size, double ratio_replace, int expiration, Boolean OBSERVE, CouchbaseClient client) 
					throws URISyntaxException, IOException, JSONException {
		double rep_items = ratio_replace * number_items;
		double tot_time = 0.0;
		int obs_true=0,obs_false=0;
		Random generator = new Random( 987654321 );
		for(int i=number_items/2 + 1;i<=((int)(number_items/2) + (int)(rep_items));i++){
			String Key = String.format("Key-%d", i);
			//String Value = String.format("%d", i);
			JSONObject Value = Jsongen.retrieveJSON(i, generator, item_size);
			try{
				OperationFuture<Boolean> repOp = null;
				if(OBSERVE){
					long preOBS = System.nanoTime();
					repOp = client.replace(Key, expiration, Value.toString(), PersistTo.MASTER);
					if(repOp.get().booleanValue())
						obs_true++;
					else
						obs_false++;
					long postOBS = System.nanoTime();
					//System.out.println("REPLACE-OBSERVE for item " + i + " :: TOOK: " + (double)(postOBS - preOBS) / 1000000.0 + " ms.");
					tot_time += (double)(postOBS - preOBS) / 1000000.0;
				}else{
					repOp = client.replace(Key, expiration, Value.toString());
				}
//				if(repOp.get().booleanValue())
//					System.out.println("Key-" + i + " has been replaced.");
//				else
//					System.out.println("Key-" + i + " wasn't replaced, for it doesn't exist.");
			} catch (Exception e) {
				System.err.println("Exception while doing replace: "
						+ e.getMessage());
			}
		}
		if (OBSERVE)
			System.out.println("No. of replaces that actually persisted to disk: " + obs_true);
			System.out.println("AVERAGE LATENCY SEEN FOR ALL REPLACES WITH OBSERVE: " + (tot_time / rep_items));
		client.shutdown(10, TimeUnit.SECONDS);
		return (tot_time / rep_items);
	}

}
