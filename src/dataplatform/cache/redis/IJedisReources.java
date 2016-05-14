package dataplatform.cache.redis;

import redis.clients.jedis.Jedis;

public interface IJedisReources {
	
	/**
	 * 获取Jedis
	 * @return	Jedis
	 */
	Jedis getJedis();
	/**
	 * 使用结束处理
	 * @param 	jedis
	 * 			jedis
	 */
	void useFinish(Jedis jedis);

}
