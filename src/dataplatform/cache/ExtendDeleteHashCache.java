package dataplatform.cache;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import dataplatform.persist.IEntityManager;

public class ExtendDeleteHashCache<V extends IHashCachedObject> extends HashCache<V> {
	
	protected final IEntityManager entityManager;
	
	protected final Queue<V> deleteQueue;

	public ExtendDeleteHashCache(String preKey, ICache cache, IEntityManager entityManager) {
		super(preKey, cache);
		this.entityManager = entityManager;
		deleteQueue = new ConcurrentLinkedQueue<V>();
	}

	@Override
	public void delete(String key, String name) {
		deleteQueue.add(get(key, name));
		super.delete(key, name);
	}
	
	public void delete() {
		entityManager.deleteSync(deleteQueue.toArray());
	}

}
