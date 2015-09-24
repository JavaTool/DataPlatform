package dataplatform.pubsub;

public interface ISimplePubsub {
	
	void publish(Object message);
	
	void subscribe(Object subscribe);
	
	void unsubscribe(Object subscribe);

}
