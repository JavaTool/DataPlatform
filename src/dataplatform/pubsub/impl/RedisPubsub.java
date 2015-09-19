package dataplatform.pubsub.impl;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;

import com.google.common.base.Preconditions;

import dataplatform.cache.redis.CacheOnJedis;
import dataplatform.pubsub.IPubsub;
import dataplatform.pubsub.IPubsubMessage;
import dataplatform.pubsub.ISubscribe;
import dataplatform.util.SerializaUtil;

public class RedisPubsub implements IPubsub {
	
	private static final Logger log = LoggerFactory.getLogger(RedisPubsub.class);
	
	private static final String REQUEST_MESSAGE = "RedisPubsubChannelRequest";
	
	private static final String SERIALIZABLE_MESSAGE = "RedisPubsubChannelSerializable";
	
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
			Preconditions.checkArgument(!channel.contains(REQUEST_MESSAGE), "illegal channel name : " + channel);
			Preconditions.checkArgument(!channel.contains(SERIALIZABLE_MESSAGE), "illegal channel name : " + channel);
			
			byte[] datas;
			if (message instanceof IPubsubMessage) {
				datas = ((IPubsubMessage) message).toByteArray();
				channel = channel + REQUEST_MESSAGE;
			} else if (message instanceof Serializable) {
				datas = SerializaUtil.serializable((Serializable) message);
				channel = channel + SERIALIZABLE_MESSAGE;
			} else {
				throw new Exception("Unknow class of message : " + message);
			}
			
			jedis.publish(SerializaUtil.serializable(channel), datas);
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
				if (channelName.endsWith(REQUEST_MESSAGE)) {
					subscribe.onMessage(channelName.replace(REQUEST_MESSAGE, ""), message);
				} else if (channelName.endsWith(SERIALIZABLE_MESSAGE)) {
					subscribe.onMessage(channelName.replace(REQUEST_MESSAGE, ""), SerializaUtil.deserializable(message));
				} else {
					throw new Exception("Unknow class of channel : " + channelName);
				}
			} catch (Exception e) {
				log.error("", e);
			}
		}
		
	}

}
