package dataplatform.collection.list;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dataplatform.cache.redis.IJedisSource;
import dataplatform.util.ArrayUtil;
import dataplatform.util.ClassUtil;
import redis.clients.jedis.Jedis;

class JedisStringList<E> implements List<E> {
	
	protected static final Logger log = LoggerFactory.getLogger(JedisStringList.class);
	
	protected final IJedisSource source;
	
	protected final String key;
	
	public JedisStringList(IJedisSource source, String key) {
		this.source = source;
		this.key = key;
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
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean contains(Object o) {
		Jedis jedis = source.getJedis();
		try {
			int len = size();
			for (int i = 0;i < len;i++) {
				String element = jedis.get(key);
				if (element.equals(o.toString())) {
					return true;
				}
			}
			return false;
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return false;
		} finally {
			source.useFinish(jedis);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<E> iterator() {
		return (Iterator<E>) this;
	}

	@Override
	public Object[] toArray() {
		Jedis jedis = source.getJedis();
		try {
			int len = size();
			Object[] ret = new Object[len];
			for (int i = 0;i < len;i++) {
				ret[i] = jedis.lindex(key, i);
			}
			return ret;
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public <T> T[] toArray(T[] a) {
		Jedis jedis = source.getJedis();
		try {
			int len = size();
			String[] ret = new String[len];
			for (int i = 0;i < len;i++) {
				ret[i] = jedis.lindex(key, i);
			}
			return ClassUtil.isNumberArray(a) ? ArrayUtil.toNumberArray(ret, a) : toArray(ret, a);
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}
	
	@SuppressWarnings("unchecked")
	private static <T> T[] toArray(String[] strs, T[] a) {
		for (int i = 0;i < a.length;i++) {
			a[i] = (T) strs[i];
		}
		return a;
	}

	@Override
	public boolean add(E e) {
		Jedis jedis = source.getJedis();
		try {
			jedis.lpush(key, e.toString());
			return true;
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
			return false;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public boolean remove(Object o) {
		Jedis jedis = source.getJedis();
		try {
			jedis.lrem(key, 1, o.toString());
			return true;
		} catch (Exception ex) {
			log.error("Error on RedisMap key : " + key, ex);
			return false;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object e : c) {
			if (!contains(e)) {
				return false;
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean addAll(Collection<? extends E> c) {
		for (Object e : c) {
			if (!add((E) e)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public E get(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public E set(int index, E element) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void add(int index, E element) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public E remove(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int indexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int lastIndexOf(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ListIterator<E> listIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		return null;
	}

}
