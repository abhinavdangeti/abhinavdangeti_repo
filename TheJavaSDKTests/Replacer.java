import java.io.IOException;
import java.net.URISyntaxException;

import net.spy.memcached.PersistTo;
import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;


public class Replacer {

	/*
	 * Method that replaces x number of items after NUM_ITEMS.
	 * Enabling OBSERVE, makes the replacer check if every item replaced has persisted or not.
	 * Replaced items have key [Key-i] and corresponding value [new-i].
	 */
	public static void replace_items(int number_items, double ratio_replace, int expiration, Boolean OBSERVE) throws URISyntaxException, IOException {
		double rep_items = ratio_replace * number_items;
		CouchbaseClient client = Mainhelper.connect();
		for(int i=number_items/2 + 1;i<=(number_items + (int)(rep_items));i++){
			try{
				OperationFuture<Boolean> repOp = null;
				if(OBSERVE){
					repOp = client.replace(String.format("Key-%d", i), expiration, String.format("new-%d", i), PersistTo.MASTER);
					assert repOp.get().booleanValue() : "Key has persisted to master";
				}else{
					repOp = client.replace(String.format("Key-%d", i), expiration, String.format("new-%d", i));
				}
				if(repOp.get().booleanValue())
					System.out.println("Key " + i + " has been replaced.");
				else
					System.out.println("Key " + i + " wasn't replaced, for it doesn't exist.");
			} catch (Exception e) {
				System.err.println("Exception while doing replace: "
						+ e.getMessage());
			}
		}
	}
}
