package dataplatform.cache;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 缓存锁
 * <p>
 * 可用于支持分布式的锁，也可以作为普通的锁。
 * {@link #lockInterruptibly()}方法和 {@link #newCondition()}方法没有实现。
 * </p>
 * @author 	fuhuiyuan
 */
public class CacheLock implements Lock {
	
	private static final Logger log = LoggerFactory.getLogger(CacheLock.class);
	
	private final ICache cache;
	
	private final String key;
	
	private final long time;
	
	public CacheLock(ICache cache, String key, long time) {
		this.cache = cache;
		this.key = key;
		this.time = time;
	}

	@Override
	public void lock() {
		while (!tryLock()) {
			try {
				long time = cache.ttl(key);
				synchronized (key) {
					if (time > 0) {
						log.info("lock wait {}.", time);
						key.wait(time);
					}
				}
			} catch (InterruptedException e) {
				log.error("", e);
			}
		}
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean tryLock() {
		String result = cache.set(key, key, ICache.SET_NX, ICache.SET_PX, time);
		return "OK".equals(result);
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		return cache.set(key, key, ICache.SET_NX, unit.equals(TimeUnit.MILLISECONDS) ? ICache.SET_PX : ICache.SET_EX, time).equals("OK");
	}

	@Override
	public void unlock() {
		cache.del(key);
	}

	@Override
	public Condition newCondition() {
		// TODO Auto-generated method stub
		return null;
	}

}
