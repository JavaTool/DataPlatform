package dataplatform.cache.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisResources implements IJedisReources {
	
	private static final Logger log = LoggerFactory.getLogger(JedisResources.class);
	
	private final JedisPool pool;
	
	public JedisResources(String address, int maxTotal, int maxIdle, long waitTime) {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(maxTotal);
		config.setMaxIdle(maxIdle);
		config.setMaxWaitMillis(waitTime);
		String[] infos = address.split(":");
		pool = new JedisPool(config, infos[0], Integer.parseInt(infos[1]));
		pool.getResource();
	}

	@Override
	public Jedis getJedis() {
		return pool.getResource();
	}

	@Override
	public void useFinish(Jedis jedis) {
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
