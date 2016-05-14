package dataplatform.cache.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class ExistsJedisReources implements IJedisReources {
	
	protected static final Logger log = LoggerFactory.getLogger(ExistsJedisReources.class);
	
	private IJedisReources resources;
	
	protected void setResouces(IJedisReources resources) {
		this.resources = resources;
	}

	@Override
	public Jedis getJedis() {
		return resources.getJedis();
	}

	@Override
	public void useFinish(Jedis jedis) {
		resources.useFinish(jedis);
	}
	
	protected void error(String msg, Throwable e) {
		log.error("", e);
	}

}
