package dataplatform.collection.map;

public interface ICountMap extends IExtendMap<String, Integer> {
	
	int incrBy(String key, int value);

}
