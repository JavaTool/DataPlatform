package dataplatform.udsl.cache;

public interface ICacheBase {
	
	/**存储类型：字符串*/
	String TYPE_STRING = "string";
	/**存储类型：列表*/
	String TYPE_LIST = "list";
	/**存储类型：集合*/
	String TYPE_SET = "set";
	/**存储类型：哈希表*/
	String TYPE_HASH = "hash";
	/**存储类型：无*/
	String TYPE_NULL = "none";
	
	void del(String key);
	
	boolean exists(String key);
	
	String[] keys(String pattern);
	
	void persist(String key);
	
	void pexpire(String key, long milliseconds);
	
	long pttl(String key);
	
	void rename(String key, String newKey);
	
	String type(String key);
	
	ICacheKeyValue getCacheKeyValue();
	
	ICacheHash getCacheHash();
	
	ICacheList getCacheList();

}
