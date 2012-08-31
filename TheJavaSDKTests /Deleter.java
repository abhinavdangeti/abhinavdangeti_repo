import java.io.IOException;
import java.net.URISyntaxException;

import net.spy.memcached.PersistTo;
import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;


public class Deleter {

	/*
	 * Method that deletes a percentage of the items that have been loaded.
	 * Enabling OBSERVE, makes the deleter check whether every item deleted has persisted even after deleting or not.
	 * Items are deleted based on the key.
	 */
	public static void delete_items(int number_items, double ratio_delete, Boolean OBSERVE) throws URISyntaxException, IOException {
		double del_items = ratio_delete * number_items;
		CouchbaseClient client = Mainhelper.connect();
		int count = 0;
		for(int i=1;i<=(int)(del_items);i++){
			try {
				OperationFuture<Boolean> delOp = null;
				if(OBSERVE){
					delOp = client.delete(String.format("Key-%d", i), PersistTo.MASTER);
					assert !delOp.get().booleanValue() : "Key has persisted to master";
				}else{
					delOp = client.delete(String.format("Key-%d", i));
				}
				if (!delOp.get().booleanValue()){
					count ++;
					//System.out.println(count + "/" + (int)(del_items));
					if(count == (int)(del_items))
						break;
				}
			} catch (Exception e) {
				System.err.println("Exception while doing delete: "
							+ e.getMessage());
			}
		}
	}
}
