package dataplatform.udsl.cache;

public class CacheOnJedis implements ICacheBase {

	@Override
	public void del(String key) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean exists(String key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String[] keys(String pattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void persist(String key) {
		// TODO Auto-generated method stub

	}

	@Override
	public void pexpire(String key, long milliseconds) {
		// TODO Auto-generated method stub

	}

	@Override
	public long pttl(String key) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void rename(String key, String newKey) {
		// TODO Auto-generated method stub

	}

	@Override
	public String type(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ICacheKeyValue getCacheKeyValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ICacheHash getCacheHash() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ICacheList getCacheList() {
		// TODO Auto-generated method stub
		return null;
	}

}
