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
		eventBus.register(getRealSubcribe(subscribe));
	}

	@Override
	public void unsubscribe(ISubscribe subscribe, String... channel) {
		eventBus.unregister(getRealSubcribe(subscribe));
	}
	
	private Object getRealSubcribe(ISubscribe subscribe) {
		return subscribe instanceof EventBusSubcribe ? ((EventBusSubcribe) subscribe).getSubscribe() : subscribe;
	}
	
	public static class EventBusSubcribe implements ISubscribe {
		
		private final Object subscribe;
		
		public EventBusSubcribe(Object subscribe) {
			this.subscribe = subscribe;
		}

		@Deprecated
		@Override
		public void onMessage(String channel, Object message) {}

		@Deprecated
		@Override
		public int getTimeout() {
			return 0;
		}

		public Object getSubscribe() {
			return subscribe;
		}

	}

}
