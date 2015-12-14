package dataplatform.collection.list;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import dataplatform.cache.redis.IJedisSource;
import dataplatform.util.ArrayUtil;
import dataplatform.util.ClassUtil;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;

class JedisStringList implements List<String> {
	
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

	@Override
	public Iterator<String> iterator() {
		return new JedisIterator();
	}
	
	private class JedisIterator implements Iterator<String> {
		
		private int cursor;
		
		private final Object[] values;
		
		public JedisIterator() {
			cursor = 0;
			values = toArray();
		}

		@Override
		public boolean hasNext() {
			return cursor < values.length;
		}

		@Override
		public String next() {
			String value = (String) values[cursor];
			cursor--;
			return value;
		}
		
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
	public boolean add(String e) {
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

	@Override
	public boolean addAll(Collection<? extends String> c) {
		for (Object e : c) {
			if (!add(e.toString())) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean addAll(int index, Collection<? extends String> c) {
		for (Object e : c) {
			set(index++, e.toString());
		}
		return true;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		for (Object e : c) {
			if (!remove(e)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		List<String> list = Lists.newArrayListWithCapacity(c.size());
		c.forEach((e) -> {
			if (contains(e)) {
				list.add(e.toString());
			}
		});
		clear();
		addAll(list);
		return true;
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
	public String get(int index) {
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

	@Override
	public String set(int index, String element) {
		Jedis jedis = source.getJedis();
		try {
			add(index, element);
			return get(index + 1);
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public void add(int index, String element) {
		Jedis jedis = source.getJedis();
		try {
			jedis.linsert(key, LIST_POSITION.BEFORE, get(index), element);
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
		} finally {
			source.useFinish(jedis);
		}
	}

	@Override
	public String remove(int index) {
		Jedis jedis = source.getJedis();
		try {
			String element = jedis.lindex(key, index);
			jedis.lrem(element, index, element);
			return element;
		} catch (Exception e) {
			log.error("Error on RedisMap key : " + key, e);
			return null;
		} finally {
			source.useFinish(jedis);
		}
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
	public ListIterator<String> listIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ListIterator<String> listIterator(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> subList(int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		return null;
	}

}
