package dataplatform.cache;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import dataplatform.persist.IEntityManager;

public class PersistHashCache<V extends IHashCachedObject> extends HashCache<V> {
	
	protected final IEntityManager entityManager;
	
	protected final Queue<V> deleteQueue;

	public PersistHashCache(String preKey, ICache cache, IEntityManager entityManager) {
		super(preKey, cache);
		this.entityManager = entityManager;
		deleteQueue = new ConcurrentLinkedQueue<V>();
	}

	public void delete(String key, String name, boolean persist) {
		V v = get(key, name);
		if (v != null) {
			if (persist) {
				deleteQueue.add(get(key, name));
			}
			super.delete(key, name);
		}
	}
	
	public void delete(String key, V hashObject, boolean persist) {
		if (persist) {
			deleteQueue.add(hashObject);
		}
		super.delete(key, hashObject);
	}
	
	public void delete() {
		entityManager.deleteSync(deleteQueue.toArray());
	}

}
