package dataplatform.pubsub.impl;

import java.util.Map;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import dataplatform.cache.redis.IJedisReources;
import dataplatform.coder.bytes.ByteCoders;
import dataplatform.coder.bytes.IBytesCoder;
import dataplatform.pubsub.IPubsub;
import dataplatform.pubsub.ISubscribe;
import dataplatform.util.SerializaUtil;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;

public class RedisPubsub implements IPubsub {
	
	private static final Logger log = LoggerFactory.getLogger(RedisPubsub.class);
	
	private static final int THREAD_COUNT = 5;
	
	private final IJedisReources cache;
	
	private final IBytesCoder coder;
	
	private final Map<ISubscribe, ListenableFuture<SubscribeThread>> subscribes;
	
	private final ListeningExecutorService listeningExecutorService;
	
	public RedisPubsub(IJedisReources cache, IBytesCoder coder) {
		this.cache = cache;
		this.coder = coder;
		listeningExecutorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(THREAD_COUNT));
		subscribes = Maps.newConcurrentMap();
	}
	
	public RedisPubsub(IJedisReources cache) {
		this(cache, ByteCoders.newSerialableCoder());
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
			SubscribeThread subscribeThread = new SubscribeThread(subscribe, stringToBytes(channel));
			ListenableFuture<SubscribeThread> future = listeningExecutorService.submit(subscribeThread, subscribeThread);
			subscribes.put(subscribe, future);
			
			log.info("subscribe : " + subscribe);
		} catch (Exception e) {
			log.error("", e);
		}
	}

	@Override
	public void unsubscribe(ISubscribe subscribe, String... channel) {
		ListenableFuture<SubscribeThread> future = subscribes.get(subscribe);
		if (future != null) {
			try {
				future.get().unsubscribe(stringToBytes(channel));
			} catch (Exception e) {
				log.error("", e);
			}
		}
	}
	
	protected byte[][] stringToBytes(String... channel) throws Exception {
		byte[][] channels = new byte[channel.length][];
		for (int i = 0;i < channel.length;i++) {
			channels[i] = SerializaUtil.serializable(channel[i]);
		}
		return channels;
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
