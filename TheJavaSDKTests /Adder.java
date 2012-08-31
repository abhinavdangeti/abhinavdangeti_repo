import java.io.IOException;
import java.net.URISyntaxException;

import net.spy.memcached.PersistTo;
import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;


public class Adder {

	/*
	 * Method that adds x number of items after NUM_ITEMS
	 * Enabling OBSERVE, makes the adder check if every item created has persisted or not.
	 * Inserted items have key [Key-i] and corresponding value [i].
	 */
	public static void add_item(int number_items, double ratio_add, int expiration, Boolean OBSERVE) throws URISyntaxException, IOException {
		double add_items = ratio_add * number_items;
		CouchbaseClient client = Mainhelper.connect();
		for(int i=number_items;i<=number_items + (int)(add_items);i++){
			try{
				OperationFuture<Boolean> addOp = null;
				if(OBSERVE){
					addOp = client.add(String.format("Key-%d", i), expiration, String.format("%d", i), PersistTo.MASTER);
					assert addOp.get().booleanValue() : "Key has persisted to master";
				}else{
					addOp = client.add(String.format("Key-%d", i), expiration, String.format("%d", i));
				}	
				if(addOp.get().booleanValue())
					System.out.println("Key-" + i + " has persisted to master");
				else
					System.out.println("Key-" + i + " already exists!");
				
			} catch (Exception e) {
				System.err.println("Exception while doing add: "
						+ e.getMessage());
			}
		}
	}
}
