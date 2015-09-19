package dataplatform.pubsub;

public interface ISubscribe {
	
	void onMessage(String channel, Object message);

}
