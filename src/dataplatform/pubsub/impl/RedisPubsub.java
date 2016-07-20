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
import dataplatform.coder.bytes.IStreamCoder;
import dataplatform.coder.bytes.StreamCoders;
import dataplatform.pubsub.IPubsub;
import dataplatform.pubsub.ISubscribe;
import redis.clients.jedis.BinaryJedisPubSub;

public class RedisPubsub implements IPubsub {
	
	private static final Logger log = LoggerFactory.getLogger(RedisPubsub.class);
	
	private static final int THREAD_COUNT = 5;
	
	private final IJedisReources cache;
	
	private final IStreamCoder coder;
	
	private final Map<ISubscribe, ListenableFuture<SubscribeThread>> subscribes;
	
	private final ListeningExecutorService listeningExecutorService;
	
	public RedisPubsub(IJedisReources cache, IStreamCoder coder) {
		this.cache = cache;
		this.coder = coder;
		listeningExecutorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(THREAD_COUNT));
		subscribes = Maps.newConcurrentMap();
	}
	
	public RedisPubsub(IJedisReources cache) {
		this(cache, StreamCoders.newProtoStuffCoder());
	}

	@Override
	public void publish(String channel, Object message) {
		Preconditions.checkNotNull(channel, "null channel name.");
		Preconditions.checkNotNull(message, "null message");
		Preconditions.checkArgument(channel.length() > 0, "zero length channel name.");

		cache.exec(jedis -> jedis.publish(coder.write(channel), coder.write(message)));
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
			channels[i] = coder.write(channel[i]);
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
			cache.exec(jedis -> jedis.subscribe(this, channels));
		}

		@Override
		public void onMessage(byte[] channel, byte[] message) {
			try {
				String channelName = coder.read(channel);
				Object object = coder.read(message);
				subscribe.onMessage(channelName, object);
			} catch (Exception e) {
				log.error("", e);
			}
		}
		
	}

}
