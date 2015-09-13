package dataplatform.cache;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 缓存器
 * @author 	fuhuiyuan
 */
public interface ICache {
	
	/**不存在时set*/
	String SET_NX = "NX";
	/**存在时set*/
	String SET_XX = "XX";
	/**set超时-秒*/
	String SET_EX = "EX";
	/**set超时-毫秒*/
	String SET_PX = "PX";
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
	
	/**
	 * 判断是否存在键
	 * @param 	key
	 * 			键
	 * @return	是否存在键
	 */
	boolean exists(Serializable key);
	/**
	 * 判断是否存在哈希名称
	 * @param 	key
	 * 			键
	 * @param 	name
	 * 			名称
	 * @return	是否存在哈希名称
	 */
	boolean hexists(Serializable key, Serializable name);
	/**
	 * 存储一个对象
	 * @param 	key
	 * 			键
	 * @param 	object
	 * 			对象
	 */
	void set(Serializable key, Serializable object);
	/**
	 * 存储一个对象
	 * @param 	key
	 * 			键
	 * @param 	value
	 * 			对象
	 * @param 	nxxx
	 * 			允许存在/不许存在
	 * @param 	expx
	 * 			秒/毫秒
	 * @param 	time
	 * 			时间
	 */
	String set(String key, String value, String nxxx, String expx, long time);
	/**
	 * 获取剩余时间
	 * @param 	key
	 * 			键
	 * @return	剩余时间
	 */
	long ttl(String key);
	/**
	 * 以哈希的方式存储一个对象
	 * @param 	key
	 * 			键
	 * @param 	name
	 * 			哈希名称
	 * @param 	object
	 * 			对象
	 */
	void hset(Serializable key, Serializable name, Serializable object);
	/**
	 * 存储多个对象
	 * @param 	map
	 * 			键值集合
	 */
	void mSet(Map<Serializable, Serializable> map);
	/**
	 * 以哈希的方式存储多个对象
	 * @param 	key
	 * 			键
	 * @param 	map
	 * 			键值集合
	 */
	void hmSet(Serializable key, Map<Serializable, Serializable> map);
	/**
	 * 删除一个键所对应的内容
	 * @param 	key
	 * 			键
	 */
	void del(Serializable key);
	/**
	 * 删除一个键所对应的内容
	 * @param 	key
	 * 			键
	 */
	void del(String key);
	/**
	 * 删除一个键所对应的哈希名称的内容
	 * @param 	key
	 * 			键
	 * @param 	name
	 * 			哈希名称
	 */
	void hdel(Serializable key, Serializable name);
	/**
	 * 删除多个键所对应的内容
	 * @param 	keys
	 * 			键集合
	 */
	void mDel(Serializable... keys);
	/**
	 * 删除一个键所对应的哈希名称的内容
	 * @param 	key
	 * 			键
	 * @param 	names
	 * 			哈希名称集合
	 */
	void hmDel(Serializable key, Serializable... names);
	/**
	 * 获取一个存储内容
	 * @param 	key
	 * 			键
	 * @return	存储内容
	 */
	Serializable get(Serializable key);
	/**
	 * 以哈希的方式获取一个存储内容
	 * @param 	key
	 * 			键
	 * @param 	name
	 * 			哈希名称
	 * @return	存储内容
	 */
	Serializable hget(Serializable key, Serializable name);
	/**
	 * 获取多个存储内容
	 * @param 	keys
	 * 			键集合
	 * @return	存储内容列表
	 */
	List<Serializable> mGet(Serializable... keys);
	/**
	 * 以哈希的方式获取多个存储内容
	 * @param 	key
	 * 			键
	 * @param 	names
	 * 			哈希名称集合
	 * @return	存储内容列表
	 */
	List<Serializable> hmGet(Serializable key, Serializable... names);
	/**
	 * 获取存储的整个哈希内容
	 * @param 	key
	 * 			建
	 * @return	整个哈希内容
	 */
	Map<Serializable, Serializable> hGetAll(Serializable key);
	/**
	 * 获取存储的整个哈希键集合
	 * @param 	key
	 * 			建
	 * @return	整个哈希键集合
	 */
	Set<Serializable> hKeys(Serializable key);
	/**
	 * 清空缓存
	 */
	void clear();
	/**
	 * 获取一个哈希的域长度
	 * @param 	key
	 * 			键
	 * @return	哈希的域长度
	 */
	long hlen(Serializable key);
	/**
	 * 返回 key 所储存的值的类型。
	 * @return	none (key不存在)、string (字符串)、list (列表)、set (集合)、hash (哈希表)
	 */
	String type(Serializable key);

}
