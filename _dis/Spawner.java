import java.util.Random;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class Spawner {
	public static JSONObject retrieveJSON(Random gen, int _itemSize) throws JSONException {
		String _number = null;
        String CHAR_LIST = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        Integer temp = gen.nextInt();
        JSONObject jsonobj = null;
        StringBuffer _padding = new StringBuffer();
        while (_padding.length() < _itemSize) {
            _padding.append(CHAR_LIST);
        }
        if(temp % 2 == 0){
            _number = "e" + temp.toString();
            jsonobj = new JSONObject("{\"planet\":\"earth\", \"species\":\"human\", \"number\":\"" + _number + "\", \"padding\":\"" + _padding + "\"}");
        } else if(temp % 3 == 0) {
            _number = "m" + temp.toString();
            jsonobj = new JSONObject("{\"planet\":\"mars\", \"species\":\"martian\", \"number\":\"" + _number + "\", \"padding\":\"" + _padding + "\"}");
        } else if(temp % 10 == 1) {
            _number = "v" + temp.toString();
            jsonobj = new JSONObject("{\"planet\":\"venus\", \"species\":\"venusses\", \"number\":\"" + _number + "\", \"padding\":\"" + _padding + "\"}");
        } else if(temp % 10 == 3) {
            _number = "j" + temp.toString();
            jsonobj = new JSONObject("{\"planet\":\"jupiter\", \"species\":\"jupitorian\", \"number\":\"" + _number + "\", \"padding\":\"" + _padding + "\"}");
        } else if(temp % 10 == 5) {
            _number = "s" + temp.toString();
            jsonobj = new JSONObject("{\"planet\":\"saturn\", \"species\":\"saturness\", \"number\":\"" + _number + "\", \"padding\":\"" + _padding + "\"}");
		} else {
            _number = "u" + temp.toString();
            jsonobj = new JSONObject("{\"planet\":\"unknown\", \"species\":\"unknown\", \"number\":\"" + _number + "\", \"padding\":\"" + _padding + "\"}");
        }
		return jsonobj;
	}

	public static JSONObject appendJSON(int i, int _appendSize) throws JSONException {
		String CHAR_LIST = "!@#$%^&*()";
		StringBuffer _padding = new StringBuffer();
		while (_padding.length() < (_appendSize - 10)) {
			_padding.append(CHAR_LIST);
		}
		JSONObject jsonobj = new JSONObject("{\"padding" + i + "\":\"" + _padding + "\"}");
		return jsonobj;
	}
}
