package dataplatform.cache.redis;

import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolResources extends JedisReources {
	
	private final JedisPool pool;
	
	public JedisPoolResources(Map<String, String> configuration) {
		this(configuration.get("cache_redisHosts"), 
				Integer.parseInt(configuration.get("cache_redisMaxConnections")), 
				Integer.parseInt(configuration.get("max_idle")), 
				Integer.parseInt(configuration.get("wait_time")));
	}
	
	public JedisPoolResources(String address, int maxTotal, int maxIdle, long waitTime) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(maxTotal);
		config.setMaxIdle(maxIdle);
		config.setMaxWaitMillis(waitTime);
		String[] infos = address.split(":");
		pool = new JedisPool(config, infos[0], Integer.parseInt(infos[1]));
		pool.getResource();
	}

	@Override
	protected Jedis getJedis() {
		return pool.getResource();
	}

	@Override
	protected void useFinish(Jedis jedis) {
		// quit
		try {
			jedis.quit();
		} catch (Exception e) {
			log.error("", e);
		}
		// close
		try {
			jedis.close();
		} catch (Exception e) {
			log.error("", e);
		}
		// disconnect
		try {
			jedis.disconnect();
		} catch (Exception e) {
			log.error("", e);
		}
	}

}
