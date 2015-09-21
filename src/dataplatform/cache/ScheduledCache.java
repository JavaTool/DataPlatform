package dataplatform.cache;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import dataplatform.persist.IEntityManager;

/**
 * 普通的计划任务缓存
 * @author 	fuhuiyuan
 */
public class ScheduledCache extends PersistenceCache {
	
	/**创建-计划任务的键队列*/
	private final Queue<String> createKeys;
	/**更新-计划任务的键队列*/
	private final Queue<String> updateKeys;
	/**删除-计划任务的键队列*/
	private final Queue<String> deleteKeys;
	/**创建-计划任务的名称队列集合*/
	private final Multimap<String, String> createHmap;
	/**更新-计划任务的名称队列集合*/
	private final Multimap<String, String> updateHmap;
	/**删除-计划任务的名称队列集合*/
	private final Multimap<String, String> deleteHmap;
	/**键的持久化后是否删除缓存的标志集合*/
	private final Map<String, Boolean> keyCaches;
	/**名称的持久化后是否删除缓存的标志集合*/
	private final Map<String, Boolean> nameCaches;
	
	public ScheduledCache(ICache cache, IEntityManager entityManager, long delay) {
		super(cache, entityManager, delay);
		
		createKeys = new ConcurrentLinkedQueue<String>();
		updateKeys = new ConcurrentLinkedQueue<String>();
		deleteKeys = new ConcurrentLinkedQueue<String>();
		createHmap = LinkedListMultimap.create();
		updateHmap = LinkedListMultimap.create();
		deleteHmap = LinkedListMultimap.create();
		keyCaches = Maps.newConcurrentMap();
		nameCaches = Maps.newConcurrentMap();
	}
	
	@Override
	protected void createSync() {
		while (createKeys.size() > 0) {
			String key = createKeys.poll();
			if (createHmap.containsKey(key)) { // HSet
				Collection<String> queue = createHmap.get(key);
				String[] names = queue.toArray(new String[queue.size()]);
				List<Object> list = hmGet(key, names);
				entityManager.createSync(list.toArray(new String[list.size()]));
				tryDeleteCache(key, queue);
			} else { // Set
				entityManager.createSync(get(key));
				tryDeleteCache(key);
			}
		}
	}
	
	/**
	 * 尝试删除缓存对象
	 * @param 	key
	 * 			键
	 * @param 	queue
	 * 			名称队列
	 */
	protected void tryDeleteCache(String key, Collection<String> queue) {
		Collection<String> waitRemoves = Lists.newLinkedList();
		for (String name : queue) {
			if (nameCaches.containsKey(name) && !nameCaches.get(name)) {
				waitRemoves.add(name);
			}
		}
		waitRemoves.removeAll(waitRemoves);
		String[] names = queue.toArray(new String[queue.size()]);
		hmDel(key, names);
	}
	
	/**
	 * 尝试删除缓存
	 * @param 	key
	 * 			键
	 */
	protected void tryDeleteCache(String key) {
		if (keyCaches.containsKey(key) && !keyCaches.get(key)) {
			del(key);
		}
	}

	@Override
	protected void updateSync() {
		while (updateKeys.size() > 0) {
			String key = updateKeys.poll();
			if (updateHmap.containsKey(key)) {
				Collection<String> queue = updateHmap.get(key);
				String[] names = queue.toArray(new String[queue.size()]);
				List<Object> list = hmGet(key, names);
				entityManager.updateSync(list.toArray(new String[list.size()]));
				tryDeleteCache(key, queue);
			} else {
				entityManager.updateSync(get(key));
				tryDeleteCache(key);
			}
		}
	}

	@Override
	protected void deleteSync() {
		while (deleteKeys.size() > 0) {
			String key = deleteKeys.poll();
			if (deleteHmap.containsKey(key)) {
				Collection<String> queue = deleteHmap.get(key);
				String[] names = queue.toArray(new String[queue.size()]);
				List<Object> list = hmGet(key, names);
				entityManager.updateSync(list.toArray(new String[list.size()]));
				hmDel(key, names);
			} else {
				entityManager.updateSync(get(key));
				del(key);
			}
		}
	}

	@Override
	public synchronized void addScheduledCreate(String key, Object value, boolean deleteCache) {
		if (deleteKeys.contains(key)) {
			deleteKeys.remove(key);
			queueAdd(updateKeys, key);
		} else {
			queueAdd(createKeys, key);
		}
		
		saveDeleteKeyCache(key, deleteCache);
		scheduledSet(key, value);
	}

	@Override
	public synchronized void addScheduledUpdate(String key, Object value, boolean deleteCache) {
		if (createKeys.contains(key)) {
			saveDeleteKeyCache(key, deleteCache);
		} else if (!deleteKeys.contains(key)) {
			queueAdd(updateKeys, key);
			saveDeleteKeyCache(key, deleteCache);
		}
		scheduledSet(key, value);
	}

	@Override
	public synchronized void addScheduledDelete(String key, Object value) {
		createKeys.remove(key);
		updateKeys.remove(key);
		queueAdd(deleteKeys, key);
		scheduledSet(key, value);
	}

	@Override
	public synchronized void addHScheduledCreate(String key, String name, Object value, boolean deleteCache) {
		if (deleteHmap.containsValue(name)) {
			addHScheduled(key, name, updateKeys, updateHmap);
		} else {
			addHScheduled(key, name, createKeys, createHmap);
		}

		saveDeleteNameCache(key, deleteCache);
		scheduledHSet(key, name, value);
	}

	@Override
	public synchronized void addHScheduledUpdate(String key, String name, Object value, boolean deleteCache) {
		if (!createHmap.containsValue(name) && !deleteHmap.containsValue(name)) {
			addHScheduled(key, name, updateKeys, updateHmap);
			saveDeleteNameCache(key, deleteCache);
		}
		scheduledHSet(key, name, value);
	}

	@Override
	public synchronized void addHScheduledDelete(String key, String name, Object value) {
		createHmap.remove(key, name);
		updateHmap.remove(key, name);
		addHScheduled(key, name, deleteKeys, deleteHmap);
		scheduledHSet(key, name, value);
	}
	
	/**
	 * 向队列中添加键
	 * @param 	queue
	 * 			队列
	 * @param 	key
	 * 			键
	 */
	protected void queueAdd(Collection<String> queue, String key) {
		if (!queue.contains(key)) {
			queue.add(key);
		}
	}
	
	/**
	 * 添加哈希任务
	 * @param 	key
	 * 			键
	 * @param 	name
	 * 			名称
	 * @param 	keys
	 * 			键队列
	 * @param 	hmap
	 * 			名称队列集合
	 */
	protected void addHScheduled(String key, String name, Queue<String> keys, Multimap<String, String> hmap) {
		queueAdd(keys, key);
		hmap.put(key, name);
	}

	/**
	 * 保存同步后指定是否删除缓存的键
	 * @param 	key
	 * 			键
	 * @param 	deleteCache
	 * 			是否删除缓存
	 * @param 	caches
	 * 			哈希缓存
	 */
	protected void saveDeleteCache(String key, boolean deleteCache, Map<String, Boolean> caches) {
		if (caches.containsKey(key) && !caches.get(key)) {
			deleteCache = false; // 之前的任务需要保留缓存对象，则这次任务也标记为保留
		}
		caches.put(key, deleteCache);
	}

	/**
	 * 保存同步后指定是否删除缓存的键
	 * @param 	key
	 * 			键
	 * @param 	deleteCache
	 * 			是否删除缓存
	 */
	protected void saveDeleteKeyCache(String key, boolean deleteCache) {
		saveDeleteCache(key, deleteCache, keyCaches);
	}
	
	/**
	 * 保存同步后指定是否删除缓存的键
	 * @param 	key
	 * 			键
	 * @param 	deleteCache
	 * 			是否删除缓存
	 */
	protected void saveDeleteNameCache(String key, boolean deleteCache) {
		saveDeleteCache(key, deleteCache, nameCaches);
	}

	/**
	 * 缓存内容
	 * @param 	key
	 * 			键
	 * @param 	value
	 * 			值
	 */
	protected void scheduledSet(String key, Object value) {
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
	protected void scheduledHSet(String key, String name, Object value) {
		if (value != null) {
			hset(key, name, value);
		}
	}

	@Override
	public void clearScheduled() {
		// TODO Auto-generated method stub
		
	}

}
