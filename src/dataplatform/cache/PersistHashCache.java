package dataplatform.cache;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import dataplatform.persist.IEntityManager;

public class PersistHashCache<V extends IHashCachedObject> extends HashCache<V> {
	
	protected final IEntityManager entityManager;
	
	protected final Queue<V> deleteQueue;

	public PersistHashCache(String preKey, ICache cache, IEntityManager entityManager, Class<V> clz) {
		super(preKey, cache, clz);
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
		if (hashObject != null) {
			if (persist) {
				deleteQueue.add(hashObject);
			}
			super.delete(key, hashObject);
		}
	}
	
	public void deleteSync() {
		int size = deleteQueue.size();
		if (size > 0) {
			Object[] values = new Object[size];
			StringBuilder builder = new StringBuilder("delete from ");
			builder.append(clz.getSimpleName()).append(" where id in (");
			for (int i = 0;i < values.length;i++) {
				values[i] = getPrimaryKey(deleteQueue.poll());
				builder.append("?,");
			}
			builder.deleteCharAt(builder.length() - 1).append(")");
			entityManager.deleteSync(builder.toString(), values);
		}
	}
	
	protected Object getPrimaryKey(V v) {
		return v.getHashName();
	}

}
