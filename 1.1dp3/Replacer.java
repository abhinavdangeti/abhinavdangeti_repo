import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.PersistTo;
import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;


public class Replacer {

	/*
	 * Method that replaces x number of items after NUM_ITEMS.
	 * Enabling OBSERVE, makes the replacer check if every item replaced has persisted or not.
	 * Replaced items have key [Key-i] and corresponding value [new-i].
	 */
	public static double replace_items(int number_items, double ratio_replace, int expiration, Boolean OBSERVE) throws URISyntaxException, IOException {
		double rep_items = ratio_replace * number_items;
		CouchbaseClient client = Mainhelper.connect();
		double tot_time = 0.0;
		int obs_true=0,obs_false=0;
		for(int i=number_items/2 + 1;i<=((int)(number_items/2) + (int)(rep_items));i++){
			try{
				OperationFuture<Boolean> repOp = null;
				if(OBSERVE){
					long preOBS = System.nanoTime();
					repOp = client.replace(String.format("Key-%d", i), expiration, String.format("new-%d", i), PersistTo.MASTER);
					if(repOp.get().booleanValue())
						obs_true++;
					else
						obs_false++;
					long postOBS = System.nanoTime();
					System.out.println("REPLACE-OBSERVE for item " + i + " :: TOOK: " + (double)(postOBS - preOBS) / 1000000.0 + " ms.");
					tot_time += (double)(postOBS - preOBS) / 1000000.0;
				}else{
					repOp = client.replace(String.format("Key-%d", i), expiration, String.format("new-%d", i));
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
