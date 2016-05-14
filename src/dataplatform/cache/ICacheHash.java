package dataplatform.cache;

import java.util.List;
import java.util.Map;

public interface ICacheHash<T, F, V> {
	
	/**
	 * 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略。
	 * @param 	key
	 * @param 	fields
	 */
	void remove(T key, Object... fields);
	/**
	 * 查看哈希表 key 中，给定域 field 是否存在。
	 * @param 	key
	 * @param 	field
	 * @return	
	 */
	boolean contains(T key, F field);
	/**
	 * 返回哈希表 key 中给定域 field 的值。
	 * @param 	key
	 * @param 	field
	 * @return	给定域的值。当给定域不存在或是给定 key 不存在时，返回 null。
	 */
	V get(T key, F field);
	/**
	 * 返回哈希表 key 中，一个或多个给定域的值。如果给定的域不存在于哈希表，那么返回一个 null 值。
	 * @param 	key
	 * @param 	fields
	 * @return	一个包含多个给定域的关联值的表，表值的排列顺序和给定域参数的请求顺序一样。
	 */
	List<V> get(T key, Object... fields);
	/**
	 * 返回哈希表 key 中，所有的域和值。
	 * @param 	key
	 * @return	
	 */
	Map<F, V> getAll(T key);
	/**
	 * 返回哈希表 key 中的所有域。
	 * @param 	key
	 * @return	一个包含哈希表中所有域的表。
	 */
	List<F> fields(T key);
	/**
	 * 返回哈希表 key 中域的数量。
	 * @param 	key
	 * @return	哈希表中域的数量。当 key 不存在时，返回 0 。
	 */
	long size(T key);
	/**
	 * 同时将多个 field-value (域-值)对设置到哈希表 key 中。
	 * 此命令会覆盖哈希表中已存在的域。
	 * 如果 key 不存在，一个空哈希表被创建并执行 HMSET 操作。
	 * @param 	key
	 * @param 	map
	 */
	void set(T key, Map<F, V> map);
	/**
	 * 将哈希表 key 中的域 field 的值设为 value 。
	 * 如果 key 不存在，一个新的哈希表被创建并进行 HSET 操作。
	 * 如果域 field 已经存在于哈希表中，旧值将被覆盖。
	 * @param 	key
	 * @param 	field
	 * @param 	value
	 */
	void set(T key, F field, V value);
	/**
	 * 返回哈希表 key 中所有域的值。
	 * @param 	key
	 * @return	一个包含哈希表中所有值的表。当 key 不存在时，返回一个空表。
	 */
	List<V> values(T key);

}
