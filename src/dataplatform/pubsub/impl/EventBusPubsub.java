package dataplatform.pubsub.impl;

import com.google.common.eventbus.EventBus;

import dataplatform.pubsub.IPubsub;
import dataplatform.pubsub.ISubscribe;

public class EventBusPubsub implements IPubsub {
	
	private final EventBus eventBus;
	
	public EventBusPubsub() {
		eventBus = new EventBus();
	}

	@Override
	public void publish(String channel, Object message) {
		eventBus.post(message);
	}

	@Override
	public void subscribe(ISubscribe subscribe, String... channel) {
		eventBus.register(subscribe);
	}

	@Override
	public void unsubscribe(ISubscribe subscribe, String... channel) {
		eventBus.unregister(subscribe);
	}

}
