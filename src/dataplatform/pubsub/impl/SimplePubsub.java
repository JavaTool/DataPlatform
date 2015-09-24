package dataplatform.pubsub.impl;

import com.google.common.eventbus.EventBus;

import dataplatform.pubsub.ISimplePubsub;

public class SimplePubsub implements ISimplePubsub {
	
	protected final EventBus pubsub;
	
	public SimplePubsub(EventBus pubsub) {
		this.pubsub = pubsub;
	}

	@Override
	public void publish(Object message) {
		pubsub.post(message);;
	}

	@Override
	public void subscribe(Object subscribe) {
		pubsub.register(subscribe);
	}

	@Override
	public void unsubscribe(Object subscribe) {
		pubsub.unregister(subscribe);
	}

}
