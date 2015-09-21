package dataplatform.pubsub.impl;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import dataplatform.cache.IStreamCoder;
import dataplatform.cache.redis.CacheOnJedis;
import dataplatform.cache.redis.CacheOnJedisPool;
import dataplatform.cache.redis.SerialableCoder;
import dataplatform.pubsub.IPubsub;
import dataplatform.pubsub.ISubscribe;
import dataplatform.util.SerializaUtil;

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
			jedis.lpush(SerializaUtil.serializable(channel), coder.write(message));
		} catch (Exception e) {
			log.error("", e);
		} finally {
			cache.useFinish(jedis);
		}
	}

	@Override
	public void subscribe(ISubscribe subscribe, String... channel) {
		executorService.execute(new SubscribeThread(subscribe, channel));
	}
	
	protected class SubscribeThread implements Runnable {
		
		private final ISubscribe subscribe;
		
		private final String[] channels;
		
		public SubscribeThread(ISubscribe subscribe, String... channels) {
			this.subscribe = subscribe;
			this.channels = channels;
		}

		@Override
		public void run() {
			Jedis jedis = cache.getJedis();
			try {
				List<byte[]> list = jedis.blpop(subscribe.getTimeout(), SerializaUtil.serializable(channels));
				while (list.size() > 0) {
					subscribe.onMessage(SerializaUtil.deserializable(list.remove(0)).toString(), coder.read(list.remove(0)));
				}
				subscribe(subscribe, channels);
			} catch (Exception e) {
				log.error("", e);
			} finally {
				cache.useFinish(jedis);
			}
		}
		
	}
	
	public static void main(String[] args) {
		CacheOnJedis cache = new CacheOnJedisPool("localhost:6379", 100, 100, 100000L);
		IStreamCoder coder = new SerialableCoder();
		IPubsub pubsub = new RedisBlockQueue(cache, coder);
		String channel = "TestSubscribe";
		String[] channels = new String[5];
		for (int i = 0;i < channels.length;i++) {
			channels[i] = channel + i;
		}
		for (int i = 0;i < 5;i++) {
			pubsub.subscribe(new Subscribe(i), channels);
		}
		for (int i = 0;i < 20;i++) {
			for (String ch : channels) {
				pubsub.publish(ch, i + " - message - " + ch);
			}
		}
		cache.del(channel);
	}
	
	private static class Subscribe implements ISubscribe {
		
		private final int id;
		
		public Subscribe(int id) {
			this.id = id;
		}

		@Override
		public void onMessage(String channel, Object message) {
			try {
				System.out.println("ID [" + id + "] CHANNEL [" + channel + "] MESSAGE [" + message +"]");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public int getTimeout() {
			return 100000;
		}
		
	}

}
