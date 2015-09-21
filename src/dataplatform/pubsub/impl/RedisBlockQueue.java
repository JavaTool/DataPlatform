package dataplatform.pubsub.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import dataplatform.cache.IStreamCoder;
import dataplatform.cache.redis.CacheOnJedis;
import dataplatform.cache.redis.SerialableCoder;
import dataplatform.pubsub.IPubsub;
import dataplatform.pubsub.ISubscribe;

public class RedisBlockQueue implements IPubsub {
	
	private static final Logger log = LoggerFactory.getLogger(RedisBlockQueue.class);
	
	private static final IStreamCoder SERIALABLE_CODER = new SerialableCoder();
	
	private final CacheOnJedis cache;
	
	private final ExecutorService executorService;
	
	public RedisBlockQueue(CacheOnJedis cache) {
		this.cache = cache;
		executorService = Executors.newSingleThreadExecutor();
	}

	@Override
	public void publish(String channel, Object message) {
		Jedis jedis = cache.getJedis();
		try {
			jedis.lpush(SERIALABLE_CODER.write(channel), SERIALABLE_CODER.write(message));
		} catch (Exception e) {
			log.error("", e);
		} finally {
			cache.useFinish(jedis);
		}
	}

	@Override
	public void subscribe(ISubscribe subscribe, String... channel) {
		for (int i = 0;i < channel.length;i++) {
			executorService.execute(new SubscribeThread(cache, subscribe, channel[i]));
		}
	}
	
	protected static class SubscribeThread implements Runnable {
		
		private final CacheOnJedis cache;
		
		private final ISubscribe subscribe;
		
		private final String channel;
		
		public SubscribeThread(CacheOnJedis cache, ISubscribe subscribe, String channel) {
			this.cache = cache;
			this.subscribe = subscribe;
			this.channel = channel;
		}

		@Override
		public void run() {
			Jedis jedis = cache.getJedis();
			try {
				byte[] message = jedis.blpop(new byte[][]{SERIALABLE_CODER.write(channel)}).remove(0);
				subscribe.onMessage(channel, message);
			} catch (Exception e) {
				log.error("", e);
			} finally {
				cache.useFinish(jedis);
			}
		}
		
	}

}
