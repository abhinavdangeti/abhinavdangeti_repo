import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.PersistTo;
import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.internal.OperationFuture;

public class Loader {

	/*
	 * Method that loads items from 1 to NUM_ITEMS.
	 * Enabling OBSERVE, makes the loader check if every item created has persisted or not.
	 * Inserted items have key [Key-i] and corresponding value [i].
	 */
	public static double load_items(int number_items, double ratio_exp, int expiration, Boolean OBSERVE) 
			throws URISyntaxException, IOException, InterruptedException, ExecutionException {
		CouchbaseClient client = Mainhelper.connect();		
		int items_with_exp = (int)(number_items * ratio_exp);
		double tot_time = 0.0;
		int obs_true=0, obs_false=0;
		for(int i=1;i<=(number_items - items_with_exp);i++){
			String Key = String.format("Key-%d", i);
			String Value = String.format("%d", i);
			if(OBSERVE){
				long preOBS = System.nanoTime();
				OperationFuture<Boolean> setOp = client.set(Key, 0, Value, PersistTo.MASTER);
				if(setOp.get().booleanValue())
					obs_true++;
				else
					obs_false++;
				long postOBS = System.nanoTime();
				System.out.println("SET-OBSERVE for item " + i + " :: TOOK: " + (double)(postOBS - preOBS) / 1000000.0 + " ms.");
				tot_time += (double)(postOBS - preOBS) / 1000000.0;
			}else{
				OperationFuture<Boolean> setOp = client.set(Key, 0, Value);
				if (setOp.get().booleanValue() == false){ 
					System.err.println("Set failed: " + setOp.getStatus().getMessage());
			    	break;
				}
			}
		}
		for(int i=(number_items - items_with_exp + 1);i<=number_items;i++){
			String Key = String.format("Key-%d", i);
			String Value = String.format("%d", i);
			if(OBSERVE){
				long preOBS = System.nanoTime();
				OperationFuture<Boolean> setOp = client.set(Key, 0, Value, PersistTo.MASTER);
				if(setOp.get().booleanValue())
					obs_true++;
				else
					obs_false++;
				long postOBS = System.nanoTime();
				System.out.println("SET-OBSERVE for item " + i + " :: TOOK: " + (double)(postOBS - preOBS) / 1000000.0 + " ms.");
				tot_time += (double)(postOBS - preOBS) / 1000000.0;
			}else{
				OperationFuture<Boolean> setOp = client.set(Key, expiration, Value);
				if (setOp.get().booleanValue() == false){ 
					System.err.println("Set failed: " + setOp.getStatus().getMessage());
			    	break;
				}
			}	
		}
		if (OBSERVE)
			System.out.println("No. of items that actually persisted to disk: " + obs_true);
			System.out.println("AVERAGE LATENCY SEEN FOR ALL SETS WITH OBSERVE: " + (tot_time / number_items));
		client.shutdown(10, TimeUnit.SECONDS);
		return (tot_time / (obs_true + obs_false));
	}
}