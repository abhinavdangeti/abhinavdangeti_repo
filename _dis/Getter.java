import java.io.IOException;
import java.net.URISyntaxException;

import com.couchbase.client.CouchbaseClient;


public class Getter {

	public static void get_em_all (int _itemCount) throws URISyntaxException, IOException {
		CouchbaseClient client = Loadrunner.connect();
		while (true) {
			for (int i=0; i<_itemCount; i++) {
				Object getObject = null;
				try {
					getObject = client.get(String.format("Key-%d", i));
				} catch (Exception e) {
					//Get didn't fetch
				}
				if (getObject == null)
					break;
			}
		}
	}
}
