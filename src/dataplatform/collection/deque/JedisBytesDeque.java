package dataplatform.collection.deque;

import java.util.AbstractQueue;
import java.util.Deque;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dataplatform.cache.redis.IJedisSource;
import dataplatform.coder.bytes.IBytesCoder;
import redis.clients.jedis.Jedis;

class JedisBytesDeque<E> extends AbstractQueue<E> implements Deque<E> {
	
	private static final Logger log = LoggerFactory.getLogger(JedisBytesDeque.class);
	
	private final IJedisSource source;
	
	private final byte[] key;
	
	private final IBytesCoder valueCoder;
	
	public JedisBytesDeque(IJedisSource source, String key, IBytesCoder keyCoder, IBytesCoder valueCoder) {
		this.source = source;
		byte[] bytes;
		try {
			bytes = keyCoder.write(key);
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			bytes = null;
		}
		this.key = bytes;
		this.valueCoder = valueCoder;
	}
	
	private byte[] writeValue(Object value) throws Exception {
		return valueCoder.write(value);
	}
	
	@SuppressWarnings("unchecked")
	private E readValue(byte[] bytes) throws Exception {
		return (E) valueCoder.read(bytes);
	}

	@Override
	public void addFirst(E e) {
		Jedis jedis = source.getJedis();
		try {
			jedis.lpush(key, writeValue(e));
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public void addLast(E e) {
		Jedis jedis = source.getJedis();
		try {
			jedis.rpush(key, writeValue(e));
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public boolean offerFirst(E e) {
		addFirst(e);
		return true;
	}

	@Override
	public boolean offerLast(E e) {
		addLast(e);
		return true;
	}

	@Override
	public E removeFirst() {
		Jedis jedis = source.getJedis();
		try {
			return readValue(jedis.lpop(key));
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public E removeLast() {
		Jedis jedis = source.getJedis();
		try {
			return readValue(jedis.rpop(key));
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public E pollFirst() {
		return removeFirst();
	}

	@Override
	public E pollLast() {
		return removeLast();
	}

	@Override
	public E getFirst() {
		Jedis jedis = source.getJedis();
		try {
			return readValue(jedis.lindex(key, 0));
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public E getLast() {
		Jedis jedis = source.getJedis();
		try {
			return readValue(jedis.lindex(key, size() - 1));
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public E peekFirst() {
		return getFirst();
	}

	@Override
	public E peekLast() {
		return getLast();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean removeFirstOccurrence(Object o) {
		return lrem((E) o, 1);
	}
	
	private boolean lrem(E e, int count) {
		Jedis jedis = source.getJedis();
		try {
			return jedis.lrem(key, count, writeValue(e)) > 0;
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
			return false;
		} finally {
			source.useFinish(jedis);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean removeLastOccurrence(Object o) {
		return lrem((E) o, -1);
	}

	@Override
	public boolean add(E e) {
		addLast(e);
		return true;
	}

	@Override
	public boolean offer(E e) {
		addLast(e);
		return true;
	}

	@Override
	public E poll() {
		return removeFirst();
	}

	@Override
	public E peek() {
		return getFirst();
	}

	@Override
	public void push(E e) {
		addFirst(e);
	}

	@Override
	public E pop() {
		return removeFirst();
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean remove(Object o) {
		return lrem((E) o, 0);
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
	public Iterator<E> iterator() {
		return new JedisIterator();
	}
	
	private class JedisIterator implements Iterator<E> {

		@Override
		public boolean hasNext() {
			return size() > 0;
		}

		@Override
		public E next() {
			return poll();
		}
		
	}

	@Override
	public Iterator<E> descendingIterator() {
		return new JedisDescendingIterator();
	}
	
	private class JedisDescendingIterator implements Iterator<E> {

		@Override
		public boolean hasNext() {
			return size() > 0;
		}

		@Override
		public E next() {
			return pop();
		}
		
	}

}
