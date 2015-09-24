package dataplatform.pubsub.impl;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import dataplatform.pubsub.IPubsub;
import dataplatform.pubsub.ISubscribe;

public class JavaBlockQueue implements IPubsub {
	
	private static final Logger log = LoggerFactory.getLogger(JavaBlockQueue.class);
	
	private static final int THREAD_COUNT = 20;
	
	private final Map<String, BlockingQueue<Object>> map;
	
	private final Table<ISubscribe, String, ListenableFuture<ISubscribe>> subscribes;
	
	private final ListeningExecutorService listeningExecutorService;
	
	public JavaBlockQueue() {
		map = Maps.newHashMap();
		listeningExecutorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(THREAD_COUNT));
		subscribes = HashBasedTable.create();
	}

	@Override
	public void publish(String channel, Object message) {
		try {
			getBlockingQueue(channel).put(message);
		} catch (InterruptedException e) {
			log.error("", e);
		}
	}

	@Override
	public void subscribe(ISubscribe subscribe, String... channel) {
		for (String key : channel) {
			SubscribeThread subscribeThread = new SubscribeThread(getBlockingQueue(key), subscribe, key);
			subscribes.put(subscribe, key, listeningExecutorService.submit(subscribeThread, subscribe));
		}
	}

	@Override
	public void unsubscribe(ISubscribe subscribe, String... channel) {
		for (String key : channel) {
			ListenableFuture<ISubscribe> future = subscribes.get(subscribe, key);
			if (future != null) {
				future.cancel(true);
			}
		}
	}
	
	private BlockingQueue<Object> getBlockingQueue(String channel) {
		if (map.containsKey(channel)) {
			BlockingQueue<Object> list = new LinkedBlockingQueue<Object>();
			map.put(channel, list);
			return list;
		} else {
			return map.get(channel);
		}
	}
	
	protected static class SubscribeThread implements Runnable {
		
		private final BlockingQueue<Object> queue;
		
		private final ISubscribe subscribe;
		
		private final String channel;
		
		public SubscribeThread(BlockingQueue<Object> queue, ISubscribe subscribe, String channel) {
			this.queue = queue;
			this.subscribe = subscribe;
			this.channel = channel;
		}

		@Override
		public void run() {
			try {
				subscribe.onMessage(channel, queue.take());
			} catch (InterruptedException e) {
				log.error("", e);
			}
		}
		
	}

}
