import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.PersistTo;
import net.spy.memcached.internal.OperationFuture;

import com.couchbase.client.CouchbaseClient;


public class Deleter {

	/*
	 * Method that deletes a percentage of the items that have been loaded.
	 * Enabling OBSERVE, makes the deleter check whether every item deleted has persisted even after deleting or not.
	 * Items are deleted based on the key.
	 */
	@SuppressWarnings("unused")
	public static double delete_items(int number_items, double ratio_delete, Boolean OBSERVE, CouchbaseClient client)
					throws URISyntaxException, IOException {
		double del_items = ratio_delete * number_items;
		int count = 0;
		double tot_time = 0.0;
		int obs_true=0,obs_false=0;
		for(int i=0;i<=(int)(del_items);i++){
			try {
				OperationFuture<Boolean> delOp = null;
				if(OBSERVE){
					long preOBS = System.nanoTime();
					delOp = client.delete(String.format("Key-%d", i), PersistTo.MASTER);
					if(delOp.get().booleanValue())
						obs_true++;
					else
						obs_false++;
					long postOBS = System.nanoTime();
					System.out.println("DELETE-OBSERVE for item " + i + " :: TOOK: " + (double)(postOBS - preOBS) / 1000000.0 + " ms.");
					tot_time += (double)(postOBS - preOBS) / 1000000.0;
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
		if (OBSERVE)
			System.out.println("No. of deletes that actually persisted: " + obs_true);
			System.out.println("AVERAGE LATENCY SEEN FOR ALL DELETES WITH OBSERVE: " + (tot_time / del_items));
		client.shutdown(10, TimeUnit.SECONDS);
		return (tot_time / del_items);
	}
}
