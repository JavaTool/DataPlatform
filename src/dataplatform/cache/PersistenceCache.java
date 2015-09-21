package dataplatform.cache;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractScheduledService;

import dataplatform.persist.IEntityManager;

/**
 * 会持久化的任务缓存
 * @author 	fuhuiyuan
 */
public abstract class PersistenceCache extends AbstractScheduledService implements IScheduledCache {
	
	protected static final Logger log = LoggerFactory.getLogger(PersistenceCache.class);
	/**缓存*/
	protected final ICache cache;
	/**持久化管理器*/
	protected final IEntityManager entityManager;
	/**运行周期*/
	protected final long delay;
	
	public PersistenceCache(ICache cache, IEntityManager entityManager, long delay) {
		this.cache = cache;
		this.entityManager = entityManager;
		this.delay = delay;
	}

	@Override
	public void registerCache(String key, @SuppressWarnings("rawtypes") Class valueClass, boolean delAtShutdown, IStreamCoder streamCoder) {
		cache.registerCache(key, valueClass, delAtShutdown, streamCoder);
	}

	@Override
	public void expire(String key, long milliseconds) {
		cache.expire(key, milliseconds);
	}

	@Override
	public boolean exists(String key) {
		return cache.exists(key);
	}

	@Override
	public boolean hexists(String key, String name) {
		return cache.hexists(key, name);
	}
	
	@Override
	public void set(String key, Object object) {
		cache.set(key, object);
	}

	@Override
	public void hset(String key, String name, Object object) {
		cache.hset(key, name, object);
	}

	@Override
	public void mSet(Map<String, Object> map) {
		cache.mSet(map);
	}

	@Override
	public void hmSet(String key, Map<String, Object> map) {
		cache.hmSet(key, map);
	}

	@Override
	public void del(String key) {
		cache.del(key);
	}

	@Override
	public void hdel(String key, String name) {
		cache.hdel(key, name);
	}

	@Override
	public void mDel(String... keys) {
		cache.mDel(keys);
	}

	@Override
	public void hmDel(String key, String... names) {
		cache.hmDel(key, names);
	}

	@Override
	public Object get(String key) {
		return cache.get(key);
	}

	@Override
	public Object hget(String key, String name) {
		return cache.hget(key, name);
	}

	@Override
	public List<Object> mGet(String... keys) {
		return cache.mGet(keys);
	}

	@Override
	public List<Object> hmGet(String key, String... names) {
		return cache.hmGet(key, names);
	}

	@Override
	public Map<String, Object> hGetAll(String key) {
		return cache.hGetAll(key);
	}

	@Override
	public Set<Object> hKeys(String key) {
		return cache.hKeys(key);
	}

	@Override
	public void clear() {
		cache.clear();
	}

	@Override
	public long hlen(String key) {
		return cache.hlen(key);
	}
	
	/**
	 * 同步创建
	 */
	protected abstract void createSync();
	/**
	 * 同步更新
	 */
	protected abstract void updateSync();
	/**
	 * 同步删除
	 */
	protected abstract void deleteSync();

	@Override
	public void shutDown() throws Exception {
		runOneIteration();
		cache.shutdown();
		super.shutDown();
		log.info("Shutdown finish.");
	}

	@Override
	public String set(String key, String value, String nxxx, String expx, long time) {
		return cache.set(key, value, nxxx, expx, time);
	}

	@Override
	public long ttl(String key) {
		return cache.ttl(key);
	}

	@Override
	public String type(String key) {
		return cache.type(key);
	}

	@Override
	protected void runOneIteration() throws Exception {
		long time = System.currentTimeMillis();
		createSync();
		updateSync();
		deleteSync();
		log.info("Sync finish use {} ms.", System.currentTimeMillis() - time);
	}

	@Override
	protected Scheduler scheduler() {
		return Scheduler.newFixedDelaySchedule(0, delay, TimeUnit.MILLISECONDS);
	}

	@Override
	public void shutdown() {
		cache.shutdown();
	}

}
