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
	
	private final CacheOnJedis cache;
	
	private final ExecutorService executorService;
	
	private final IStreamCoder coder;
	
	public RedisBlockQueue(CacheOnJedis cache, IStreamCoder coder) {
		this.cache = cache;
		this.coder = coder;
		executorService = Executors.newSingleThreadExecutor();
	}
	
	public RedisBlockQueue(CacheOnJedis cache) {
		this(cache, new SerialableCoder());
	}

	@Override
	public void publish(String channel, Object message) {
		Jedis jedis = cache.getJedis();
		try {
			jedis.lpush(coder.write(channel), coder.write(message));
		} catch (Exception e) {
			log.error("", e);
		} finally {
			cache.useFinish(jedis);
		}
	}

	@Override
	public void subscribe(ISubscribe subscribe, String... channel) {
		for (int i = 0;i < channel.length;i++) {
			executorService.execute(new SubscribeThread(subscribe, channel[i]));
		}
	}
	
	protected class SubscribeThread implements Runnable {
		
		private final ISubscribe subscribe;
		
		private final String channel;
		
		public SubscribeThread(ISubscribe subscribe, String channel) {
			this.subscribe = subscribe;
			this.channel = channel;
		}

		@Override
		public void run() {
			Jedis jedis = cache.getJedis();
			try {
				byte[][] keys = new byte[][]{coder.write(channel)};
				byte[] message = jedis.blpop(subscribe.getTimeout(), keys).remove(0);
				subscribe.onMessage(channel, message);
			} catch (Exception e) {
				log.error("", e);
			} finally {
				cache.useFinish(jedis);
			}
		}
		
	}

}
