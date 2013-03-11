import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import net.spy.memcached.PersistTo;
import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;


public class Adder {

	/*
	 * Method that adds x number of items after NUM_ITEMS
	 * Enabling OBSERVE, makes the adder check if every item created has persisted or not.
	 * Inserted items have key [Key-i] and corresponding json document [i].
	 */
	@SuppressWarnings("unused")
	public static double add_items(int number_items, int item_size, double ratio_add, int expiration, Boolean OBSERVE, CouchbaseClient client) 
					throws URISyntaxException, IOException, JSONException {
		double add_items = ratio_add * number_items;
		double tot_time = 0.0;
		int obs_true=0, obs_false=0;
		Random generator = new Random( 123456789 );
		for(int i=number_items;i<=number_items + (int)(add_items);i++){
			String Key = String.format("Key-%d", i);
			//String Value = String.format("%d", i);
			JSONObject Value = Jsongen.retrieveJSON(i, generator, item_size);
			try{
				OperationFuture<Boolean> addOp = null;
				if(OBSERVE){
					long preOBS = System.nanoTime();
					addOp = client.add(Key, expiration, Value.toString(), PersistTo.MASTER);
					if(addOp.get().booleanValue())
						obs_true++;
					else
						obs_false++;
					long postOBS = System.nanoTime();
					//System.out.println("ADD-OBSERVE for item " + i + " :: TOOK: " + (double)(postOBS - preOBS) / 1000000.0 + " ms.");
					tot_time +=  (double)(postOBS - preOBS) / 1000000.0;
				}else{
					addOp = client.add(Key, expiration, Value.toString());
				}	
//				if(addOp.get().booleanValue())
//					System.out.println("Key-" + i + " has persisted to master");
//				else
//					System.out.println("Key-" + i + " already exists!");

				
			} catch (Exception e) {
				System.err.println("Exception while doing add: "
						+ e.getMessage());
			}
		}
		if(OBSERVE)
			System.out.println("No. of added items that actually persisted to disk: " + obs_true);
			System.out.println("AVERAGE LATENCY SEEN FOR ALL ADDS WITH OBSERVE: " + (tot_time / add_items));
		client.shutdown(10, TimeUnit.SECONDS);
		return (tot_time / add_items);
	}
	
}
