package dataplatform.cache.redis;

import redis.clients.jedis.Jedis;

public interface IJedisSource {
	
	Jedis getJedis();
	
	void useFinish(Jedis jedis);

}
