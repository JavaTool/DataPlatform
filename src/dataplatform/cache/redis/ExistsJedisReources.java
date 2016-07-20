package dataplatform.cache.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExistsJedisReources implements IJedisReources {
	
	private static final Logger log = LoggerFactory.getLogger(ExistsJedisReources.class);
	
	private IJedisReources resources;
	
	protected void setResouces(IJedisReources resources) {
		this.resources = resources;
	}
	
	protected void error(String msg, Throwable e) {
		log.error("", e);
	}
	
	@Override
	public <T> T exec(RedisExecutor<T> run, T param) {
		return resources.exec(run, param);
	}

	@Override
	public void exec(VoidRedisExecutor run) {
		resources.exec(run);
	}

}
