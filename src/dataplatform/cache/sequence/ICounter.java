package dataplatform.cache.sequence;

import java.io.Serializable;

/**
 * 计数器
 * @author 	fuhuiyuan
 */
public interface ICounter {
	
	/**
	 * 获得一个当前计数
	 * @param 	key
	 * 			计数名称
	 * @return	当前计数
	 */
	long getCount(Serializable key);
	/**
	 * 增加计数
	 * @param 	key
	 * 			计数名称
	 * @param 	value
	 * 			增量
	 * @return	当前计数
	 */
	long incr(Serializable key, long value);
	/**
	 * 减少计数
	 * @param 	key
	 * 			计数名称
	 * @param 	value
	 * 			减量
	 * @return	当前计数
	 */
	long decr(Serializable key, long value);
	/**
	 * 删除一个计数
	 * @param 	key
	 * 			计数名称
	 */
	void deleteCount(Serializable key);

}
