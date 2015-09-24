package dataplatform.pubsub;

public interface IPubsub {
	
	void publish(String channel, Object message);
	
	void subscribe(ISubscribe subscribe, String... channel);
	
	void unsubscribe(ISubscribe subscribe, String... channel);

}
