package dataplatform.pubsub.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import dataplatform.cache.redis.IJedisReources;
import dataplatform.coder.bytes.IStreamCoder;
import dataplatform.coder.bytes.StreamCoders;
import dataplatform.pubsub.IPubsub;
import dataplatform.pubsub.ISubscribe;

public class RedisBlockQueue implements IPubsub {
	
	private static final Logger log = LoggerFactory.getLogger(RedisBlockQueue.class);
	
	private static final int THREAD_COUNT = 5;
	
	private static final List<byte[]> EMPTY_LIST = ImmutableList.of();
	
	private final IJedisReources cache;
	
	private final IStreamCoder coder;
	
	private final Map<ISubscribe, ListenableFuture<SubscribeThread>> subscribes;
	
	private final ListeningExecutorService listeningExecutorService;
	
	public RedisBlockQueue(IJedisReources cache, IStreamCoder coder) {
		this.cache = cache;
		this.coder = coder;
		listeningExecutorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(THREAD_COUNT));
		subscribes = Maps.newConcurrentMap();
	}
	
	public RedisBlockQueue(IJedisReources cache) {
		this(cache, StreamCoders.newProtoStuffCoder());
	}

	@Override
	public void publish(String channel, Object message) {
		cache.exec(jedis -> jedis.lpush(coder.write(channel), coder.write(message)));
	}

	@Override
	public void subscribe(ISubscribe subscribe, String... channel) {
		SubscribeThread subscribeThread = new SubscribeThread(subscribe, channel);
		ListenableFuture<SubscribeThread> future = listeningExecutorService.submit(subscribeThread, subscribeThread);
		subscribes.put(subscribe, future);
	}

	@Override
	public void unsubscribe(ISubscribe subscribe, String... channel) {
		ListenableFuture<SubscribeThread> future = subscribes.get(subscribe);
		if (future != null) {
			try {
				future.get().removeChannels(future, channel);
			} catch (Exception e) {
				log.error("", e);
			}
		}
	}
	
	protected class SubscribeThread implements Runnable {
		
		private final ISubscribe subscribe;
		
		private final String[] channels;
		
		public SubscribeThread(ISubscribe subscribe, String... channels) {
			this.subscribe = subscribe;
			this.channels = channels;
		}
		
		public void removeChannels(ListenableFuture<SubscribeThread> future, String... channels) {
			// stop thread
			future.cancel(true);
			// remove channel
			List<String> list = Lists.newArrayList(this.channels);
			for (String channel : channels) {
				list.remove(channel);
			}
			// start new thread
			String[] newChannels = list.toArray(new String[list.size()]);
			if (newChannels.length > 0) {
				subscribe(subscribe, newChannels);
			}
		}

		@Override
		public void run() {
			try {
				List<byte[]> list = cache.exec(jedis -> {
					return jedis.blpop(subscribe.getTimeout(), coder.write(channels));
				}, EMPTY_LIST);
				while (list.size() > 0) {
					subscribe.onMessage(coder.write(list.remove(0)).toString(), coder.read(list.remove(0)));
				}
				subscribe(subscribe, channels);
			} catch (Exception e) {
				log.error("", e);
			}
		}
		
	}

}
