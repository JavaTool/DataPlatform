package dataplatform.pubsub.impl;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import dataplatform.pubsub.IPubsub;
import dataplatform.pubsub.ISubscribe;

public class JavaBlockQueue implements IPubsub {
	
	private static final Logger log = LoggerFactory.getLogger(JavaBlockQueue.class);
	
	private final Map<String, BlockingQueue<Object>> map;
	
	private final ExecutorService executorService;
	
	public JavaBlockQueue() {
		map = Maps.newHashMap();
		executorService = Executors.newSingleThreadExecutor();
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
			executorService.execute(new SubscribeThread(getBlockingQueue(key), subscribe, key));
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