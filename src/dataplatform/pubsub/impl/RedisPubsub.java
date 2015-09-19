package dataplatform.pubsub.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;

import com.google.common.base.Preconditions;

import dataplatform.cache.IStreamCoder;
import dataplatform.cache.redis.CacheOnJedis;
import dataplatform.cache.redis.SerialableCoder;
import dataplatform.pubsub.IPubsub;
import dataplatform.pubsub.ISubscribe;
import dataplatform.util.SerializaUtil;

public class RedisPubsub implements IPubsub {
	
	private static final Logger log = LoggerFactory.getLogger(RedisPubsub.class);
	
	private static final IStreamCoder SERIALABLE_CODER = new SerialableCoder();
	
	private final CacheOnJedis cache;
	
	public RedisPubsub(CacheOnJedis cache) {
		this.cache = cache;
	}

	@Override
	public void publish(String channel, Object message) {
		Jedis jedis = cache.getJedis();
		try {
			Preconditions.checkNotNull(channel, "null channel name.");
			Preconditions.checkNotNull(message, "null message");
			Preconditions.checkArgument(channel.length() > 0, "zero length channel name.");

			byte[] datas = SERIALABLE_CODER.write(message);
			jedis.publish(SERIALABLE_CODER.write(channel), datas);
		} catch (Exception e) {
			log.error("", e);
		} finally {
			cache.useFinish(jedis);
		}
	}

	@Override
	public void subscribe(ISubscribe subscribe, String... channel) {
		try {
			byte[][] channels = new byte[channel.length][];
			for (int i = 0;i < channel.length;i++) {
				channels[i] = SerializaUtil.serializable(channel[i]);
			}
			
			new Thread(new SubscribeThread(subscribe, channels, cache), "RedisPubsub : " + subscribe).start();
			log.info("subscribe : " + subscribe);
		} catch (Exception e) {
			log.error("", e);
		}
	}
	
	protected static class SubscribeThread implements Runnable {
		
		private final ISubscribe subscribe;
		
		private final byte[][] channels;
		
		private final CacheOnJedis cache;
		
		public SubscribeThread(ISubscribe subscribe, byte[][] channels, CacheOnJedis cache) {
			this.subscribe = subscribe;
			this.channels = channels;
			this.cache = cache;
		}

		@Override
		public void run() {
			Jedis jedis = cache.getJedis();
			try {
				jedis.subscribe(new Subscribe(subscribe), channels);
			} catch (Exception e) {
				log.error("", e);
			} finally {
				cache.useFinish(jedis);
			}
		}
		
	}
	
	protected static class Subscribe extends BinaryJedisPubSub {
		
		private final ISubscribe subscribe;
		
		public Subscribe(ISubscribe subscribe) {
			this.subscribe = subscribe;
		}

		@Override
		public void onMessage(byte[] channel, byte[] message) {
			try {
				String channelName = (String) SerializaUtil.deserializable(channel);
				Object object = SERIALABLE_CODER.read(message);
				subscribe.onMessage(channelName, object);
			} catch (Exception e) {
				log.error("", e);
			}
		}
		
	}

}
