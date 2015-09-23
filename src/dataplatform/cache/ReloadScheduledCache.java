package dataplatform.cache;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.Lists;

import dataplatform.persist.IEntityManager;

/**
 * 可以重加载的任务缓存器
 * @author 	fuhuiyuan
 */
public class ReloadScheduledCache extends PersistenceCache {
	
	/**创建同步器*/
	private final Sync createSync;
	/**更新同步器*/
	private final Sync updateSync;
	/**删除同步器*/
	private final Sync deleteSync;
	/**同步锁*/
	private final Lock lock;
	
	public ReloadScheduledCache(ICache cache, IEntityManager entityManager, String preKey, long delay, long lockTime) {
		super(cache, entityManager, delay);
		
		createSync = makeCreateSync(preKey);
		updateSync = makeUpdateSync(preKey);
		deleteSync = makeDeleteSync(preKey);

		lock = new CacheLock(cache, preKey + "_CacheLock", lockTime);
	}
	
	private void sync(Sync sync, boolean deleteAll) {
		String syncKey = sync.getKey();
		lock.lock(); // 锁住
		try {
			Map<String, Object> keyMap = hGetAll(syncKey); // 获取要操作的键集合
			List<String> deleteKeyList = Lists.newLinkedList(); // 用来保存要删除的缓存键
			List<Object> syncList = Lists.newLinkedList(); // 用来保存要进行持久化的对象
			for (String _key : keyMap.keySet()) {
				if (sync.isHKey(_key)) { // HSet
					String _HKey = sync.makeHKey(_key); // 转化为对应的键
					Map<String, Object> nameMap = hGetAll(_HKey); // 获取要操作的名称集合
					Set<String> names = nameMap.keySet();
					syncList.addAll(hmGet(_key, names.toArray(new String[names.size()]))); // 保存需要持久化的缓存对象
					List<String> deteleNameList = Lists.newLinkedList(); // 用来保存要删除的缓存名称
					for (String name : names) {
						if (deleteAll || (Boolean) nameMap.get(name)) {
							deteleNameList.add(name); // 保存要删除的缓存名称
						}
					}
					del(_HKey);
					hmDel(_key, deteleNameList.toArray(new String[deteleNameList.size()])); // 删除所有缓存的哈希对象
				} else { // set
					String type = type(_key);
					switch (CacheType.valueOf(type)) {
					case string : 
						Object object = get(_key);
						if (object != null) {
							syncList.add(object); // 保存需要持久化的缓存对象
						}
						break;
					case hash:
						Map<String, Object> all = hGetAll(_key);
						for (Object value : all.values()) {
							syncList.add(value); // 保存需要持久化的缓存对象
						}
						break;
					default:
						break;
					}
					
					if (deleteAll || (Boolean) keyMap.get(_key)) {
						deleteKeyList.add(_key); // 保存要删除的缓存键
					}
				}
			}
			sync.execute(syncList.toArray(new Object[syncList.size()])); // 进行持久化操作
			del(syncKey);
			mDel(deleteKeyList.toArray(new String[deleteKeyList.size()])); // 删除不需要缓存的对象
		} finally {
			lock.unlock(); // 解锁
		}
	}
	
	/**
	 * 生成一个创建同步器
	 * @param 	preKey
	 * 			键前缀
	 * @return	创建同步器
	 */
	protected Sync makeCreateSync(String preKey) {
		return new CreateSync(preKey);
	}
	
	/**
	 * 生成一个更新同步器
	 * @param 	preKey
	 * 			键前缀
	 * @return	更新同步器
	 */
	protected Sync makeUpdateSync(String preKey) {
		return new UpdateSync(preKey);
	}
	
	/**
	 * 生成一个删除同步器
	 * @param 	preKey
	 * 			键前缀
	 * @return	删除同步器
	 */
	protected Sync makeDeleteSync(String preKey) {
		return new DeleteSync(preKey);
	}
	
	/**
	 * 同步器
	 * @author 	fuhuiyuan
	 */
	protected abstract class Sync {
		
		/**同步键*/
		protected final String key;
		
		public Sync(String key) {
			this.key = key;
		}
		
		/**
		 * 是否存储的是哈希结构
		 * @param 	key
		 * 			键
		 * @return	是否存储的是哈希结构
		 */
		public boolean isHKey(String key) {
			return exists(makeHKey(key));
		}
		
		/**
		 * 执行同步
		 * @param 	entity
		 * 			实体
		 */
		public abstract void execute(Object[] entity);
		/**
		 * 生成哈希键
		 * @param 	key
		 * 			键
		 * @return	哈希键
		 */
		public abstract String makeHKey(String key);
		
		/**
		 * 获取同步键
		 * @return	同步键
		 */
		public String getKey() {
			return key;
		}
		
	}
	
	/**
	 * 创建同步器
	 * @author 	fuhuiyuan
	 */
	protected class CreateSync extends Sync {
		
		public CreateSync(String preKey) {		
			super("createKey_" + preKey);
		}

		@Override
		public void execute(Object[] entity) {
			entityManager.createSync(entity);
		}

		@Override
		public String makeHKey(String key) {
			return "Create_H_" + key;
		}
		
	}

	/**
	 * 更新同步器
	 * @author 	fuhuiyuan
	 */
	protected class UpdateSync extends Sync {
		
		public UpdateSync(String preKey) {		
			super("updateKey_" + preKey);
		}

		@Override
		public void execute(Object[] entity) {
			entityManager.updateSync(entity);
		}

		@Override
		public String makeHKey(String key) {
			return "Update_H_" + key;
		}
		
	}

	/**
	 * 删除同步器
	 * @author 	fuhuiyuan
	 */
	protected class DeleteSync extends Sync {
		
		public DeleteSync(String preKey) {		
			super("deleteKey_" + preKey);
		}

		@Override
		public void execute(Object[] entity) {
			entityManager.updateSync(entity);
		}

		@Override
		public String makeHKey(String key) {
			return "Delete_H_" + key;
		}
		
	}
	
	@Override
	protected void createSync() {
		sync(createSync, false);
	}

	@Override
	protected void updateSync() {
		sync(updateSync, false);
	}

	@Override
	protected void deleteSync() {
		sync(deleteSync, false);
	}

	@Override
	public synchronized void addScheduledCreate(String key, Object value, boolean deleteCache) {
		scheduledSet(key, value);
		lock.lock(); // 锁住
		try {
			if (hexists(deleteSync.getKey(), key)) {
				hdel(deleteSync.getKey(), key);
				hset(updateSync.getKey(), key, deleteCache);
			} else {
				addScheduled(createSync, key, deleteCache);
			}
		} finally {
			lock.unlock(); // 解锁
		}
	}

	@Override
	public synchronized void addScheduledUpdate(String key, Object value, boolean deleteCache) {
		scheduledSet(key, value);
		lock.lock(); // 锁住
		try {
			if (hexists(createSync.getKey(), key)) {
				addScheduled(createSync, key, deleteCache);
			} else if (!hexists(deleteSync.getKey(), key)) {
				addScheduled(updateSync, key, deleteCache);
			}
		} finally {
			lock.unlock(); // 解锁
		}
	}

	@Override
	public synchronized void addScheduledDelete(String key, Object value) {
		scheduledSet(key, value);
		lock.lock(); // 锁住
		try {
			hdel(createSync.getKey(), key);
			hdel(updateSync.getKey(), key);
			addScheduled(deleteSync, key, true);
		} finally {
			lock.unlock(); // 解锁
		}
	}

	@Override
	public synchronized void addHScheduledCreate(String key, String name, Object value, boolean deleteCache) {
		scheduledHSet(key, name, value);
		lock.lock(); // 锁住
		try {
			if (hexists(deleteSync.makeHKey(key), name)) {
				hdel(deleteSync.makeHKey(key), name);
				addHScheduled(updateSync, key, name, deleteCache);
			} else {
				addHScheduled(createSync, key, name, deleteCache);
			}
		} finally {
			lock.unlock(); // 解锁
		}
	}

	@Override
	public synchronized void addHScheduledUpdate(String key, String name, Object value, boolean deleteCache) {
		scheduledHSet(key, name, value);
		lock.lock(); // 锁住
		try {
			if (hexists(createSync.makeHKey(key), name)) {
				addHScheduled(createSync, key, name, deleteCache);
			} else if (!hexists(deleteSync.makeHKey(key), name)) {
				addHScheduled(updateSync, key, name, deleteCache);
			}
		} finally {
			lock.unlock(); // 解锁
		}
	}

	@Override
	public synchronized void addHScheduledDelete(String key, String name, Object value) {
		lock.lock(); // 锁住
		try {
			hdel(createSync.makeHKey(key), name);
			hdel(updateSync.makeHKey(key), name);
			addHScheduled(deleteSync, key, name, true);
			scheduledHSet(key, name, value);
		} finally {
			lock.unlock(); // 解锁
		}
	}

	/**
	 * 缓存内容
	 * @param 	sync
	 * 			同步器
	 * @param 	key
	 * 			键
	 * @param 	deleteCache
	 * 			同步后是否删除缓存
	 */
	private void addScheduled(Sync sync, String key, boolean deleteCache) {
		String syncKey = sync.getKey();
		if (hexists(syncKey, key)) {
			if (!(Boolean) hget(syncKey, key)) {
				deleteCache = false;
			}
		}
		hset(syncKey, key, deleteCache);
	}

	/**
	 * 缓存哈希内容
	 * @param 	sync
	 * 			同步器
	 * @param 	key
	 * 			键
	 * @param 	name
	 * 			名称
	 * @param 	deleteCache
	 * 			同步后是否删除缓存
	 */
	private void addHScheduled(Sync sync, String key, String name, boolean deleteCache) {
		hset(sync.getKey(), key, false);
		hset(sync.makeHKey(key), name, deleteCache);
	}

	/**
	 * 缓存内容
	 * @param 	key
	 * 			键
	 * @param 	value
	 * 			值
	 */
	private void scheduledSet(String key, Object value) {
		if (value != null) {
			set(key, value);
		}
	}
	
	/**
	 * 缓存哈希内容
	 * @param 	key
	 * 			键
	 * @param 	name
	 * 			名称
	 * @param 	value
	 * 			值
	 */
	private void scheduledHSet(String key, String name, Object value) {
		if (value != null) {
			hset(key, name, value);
		}
	}

	@Override
	public void clearScheduled() {
		sync(createSync, true);
		sync(updateSync, true);
		sync(deleteSync, true);
	}

}
