package dataplatform.pubsub.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
	
	private final CacheOnJedis cache;
	
	private final ExecutorService executorService;
	
	private final IStreamCoder coder;
	
	public RedisPubsub(CacheOnJedis cache, IStreamCoder coder) {
		this.cache = cache;
		this.coder = coder;
		executorService = Executors.newSingleThreadExecutor();
	}
	
	public RedisPubsub(CacheOnJedis cache) {
		this(cache, new SerialableCoder());
	}

	@Override
	public void publish(String channel, Object message) {
		Jedis jedis = cache.getJedis();
		try {
			Preconditions.checkNotNull(channel, "null channel name.");
			Preconditions.checkNotNull(message, "null message");
			Preconditions.checkArgument(channel.length() > 0, "zero length channel name.");

			jedis.publish(SerializaUtil.serializable(channel), coder.write(message));
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
			
			executorService.execute(new SubscribeThread(subscribe, channels));
			log.info("subscribe : " + subscribe);
		} catch (Exception e) {
			log.error("", e);
		}
	}
	
	protected class SubscribeThread extends BinaryJedisPubSub implements Runnable {
		
		private final ISubscribe subscribe;
		
		private final byte[][] channels;
		
		public SubscribeThread(ISubscribe subscribe, byte[][] channels) {
			this.subscribe = subscribe;
			this.channels = channels;
		}

		@Override
		public void run() {
			Jedis jedis = cache.getJedis();
			try {
				jedis.subscribe(this, channels);
			} catch (Exception e) {
				log.error("", e);
			} finally {
				cache.useFinish(jedis);
			}
		}

		@Override
		public void onMessage(byte[] channel, byte[] message) {
			try {
				String channelName = (String) SerializaUtil.deserializable(channel);
				Object object = coder.read(message);
				subscribe.onMessage(channelName, object);
			} catch (Exception e) {
				log.error("", e);
			}
		}
		
	}

}
