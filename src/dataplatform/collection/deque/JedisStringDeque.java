package dataplatform.collection.deque;

import java.util.AbstractQueue;
import java.util.Deque;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import dataplatform.cache.redis.IJedisSource;
import redis.clients.jedis.Jedis;

class JedisStringDeque extends AbstractQueue<String> implements Deque<String> {
	
	protected static final Logger log = LoggerFactory.getLogger(JedisStringDeque.class);
	
	protected final IJedisSource source;
	
	protected final String key;
	
	public JedisStringDeque(IJedisSource source, String key) {
		this.source = source;
		this.key = key;
	}

	@Override
	public void clear() {
		Jedis jedis = source.getJedis();
		try {
			if ("list".equals(jedis.type(key))) {
				jedis.del(key);
			}
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public void addFirst(String e) {
		e = Preconditions.checkNotNull(e);
		Jedis jedis = source.getJedis();
		try {
			jedis.lpush(key, e);
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public void addLast(String e) {
		e = Preconditions.checkNotNull(e);
		Jedis jedis = source.getJedis();
		try {
			jedis.rpush(key, e);
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public boolean offerFirst(String e) {
		addFirst(e);
		return true;
	}

	@Override
	public boolean offerLast(String e) {
		addLast(e);
		return true;
	}

	@Override
	public String removeFirst() {
		Jedis jedis = source.getJedis();
		try {
			return jedis.lpop(key);
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public String removeLast() {
		Jedis jedis = source.getJedis();
		try {
			return jedis.rpop(key);
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public String pollFirst() {
		return removeFirst();
	}

	@Override
	public String pollLast() {
		return removeLast();
	}

	@Override
	public String getFirst() {
		return get(0);
	}

	@Override
	public String getLast() {
		return get(size() - 1);
	}

	@Override
	public String peekFirst() {
		return getFirst();
	}

	@Override
	public String peekLast() {
		return getLast();
	}

	@Override
	public boolean removeFirstOccurrence(Object o) {
		return lrem(o.toString(), 1);
	}
	
	private boolean lrem(String e, int count) {
		e = Preconditions.checkNotNull(e);
		Jedis jedis = source.getJedis();
		try {
			return jedis.lrem(key, count, e) > 0;
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
			return false;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public boolean removeLastOccurrence(Object o) {
		return lrem(o.toString(), -1);
	}

	@Override
	public boolean remove(Object o) {
		return lrem(o.toString(), 0);
	}

	@Override
	public boolean offer(String e) {
		addLast(e);
		return true;
	}

	@Override
	public String poll() {
		return removeFirst();
	}

	@Override
	public String peek() {
		return getFirst();
	}

	@Override
	public void push(String e) {
		addFirst(e);
	}

	@Override
	public String pop() {
		return removeLast();
	}

	@Override
	public int size() {
		Jedis jedis = source.getJedis();
		try {
			return jedis.llen(key).intValue();
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return 0;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public Iterator<String> iterator() {
		return new JedisIterator();
	}
	
	private String get(int index) {
		Jedis jedis = source.getJedis();
		try {
			return jedis.lindex(key, index);
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}
	
	private class JedisIterator implements Iterator<String> {
		
		private int curor;

		@Override
		public boolean hasNext() {
			return size() > curor;
		}

		@Override
		public String next() {
			return get(curor++);
		}
		
	}

	@Override
	public Iterator<String> descendingIterator() {
		return new JedisDescendingIterator();
	}
	
	private class JedisDescendingIterator implements Iterator<String> {
		
		private int curor;
		
		public JedisDescendingIterator() {
			curor = size() - 1;
		}

		@Override
		public boolean hasNext() {
			return curor > 0;
		}

		@Override
		public String next() {
			return get(curor--);
		}
		
	}

}
