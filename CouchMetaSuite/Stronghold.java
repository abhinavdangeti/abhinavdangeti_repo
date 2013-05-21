
public class Stronghold {

    private int _memQuota;
    private boolean _json;
    private int _itemCount;
    private int _itemSize;
    private String _prefix;
    private double _expRatio;
    private int _expiration;
    private double _delRatio;
    private int _addCount;

    public Stronghold () {
	_memQuota = 2000;
	_json = false;
	_itemCount = 10000;
	_itemSize = 256;
	_prefix = "";
	_expRatio = 0.0;
	_expiration = 0;
	_delRatio = 0.0;
	_addCount = 0;
    }

    public void setMemquota(int quota) {
	_memQuota = quota;
    }

    public void setJson(boolean flag) {
	_json = flag;
    }

    public void setItemcount(int count) {
	_itemCount = count;
    }

    public void setSize(int size) {
	_itemSize = size;
    }

    public void setPrefix(String pre) {
	_prefix = pre;
    }

    public void setExpRatio(double expires) {
	_expRatio = expires;
    }

    public void setExpiration(int exp) {
	_expiration = exp;
    }

    public void setDelRatio(double del) {
	_delRatio = del;
    }

    public void setAddCount(int addcount) {
	_addCount = addcount;
    }

    public int getMemquota() {
	return _memQuota;
    }

    public boolean isJson() {
	return _json;
    }

    public int getItemcount() {
	return _itemCount;
    }

    public int getItemsize() {
	return _itemSize;
    }

    public String getPrefix() {
	return _prefix;
    }

    public double getExpRatio() {
	return _expRatio;
    }

    public int getExpiration() {
	return _expiration;
    }

    public double getDelRatio() {
	return _delRatio;
    }

    public int getAddCount() {
	return _addCount;
    }
}
