package dataplatform.cache.redis;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import dataplatform.cache.ICache;
import dataplatform.cache.ICacheUnit;
import dataplatform.cache.IStreamCoder;
import dataplatform.util.SerializaUtil;

/**
 * Jedis缓存器
 * @author 	fuhuiyuan
 */
public abstract class CacheOnJedis implements ICache {
	
	protected static final Logger log = LoggerFactory.getLogger(CacheOnJedis.class);
	
	protected final Map<Serializable, ICacheUnit> cacheUnits = Maps.newConcurrentMap();
	
	protected final ICacheExecutor setExecutor = createSetExecutor();
	
	protected final ICacheExecutor hsetExecutor = new HsetExecutor();
	
	protected final ICacheExecutor delExecutor = new DelExecutor();
	
	protected final ICacheExecutor getExecutor = createGetExecutor();
	
	protected final ICacheExecutor hgetExecutor = new HgetExecutor();
	
	protected final ICacheExecutor hdelExecutor = new HdelExecutor();
	
	protected final ICacheExecutor hlenAndHgetExecutor = new HlenAndHgetAllExecutor();
	
	protected final ICacheExecutor hkeysAndTtlExecutor = new HkeysAndTtlExecutor();
	
	protected final ICacheExecutor typeExecutor = new TypeExecutor();
	
	protected final ICacheExecutor existsExecutor = new ExistsExecutor();
	
	/**
	 * 获取Jedis
	 * @return	Jedis
	 */
	public abstract Jedis getJedis();
	/**
	 * 使用结束处理
	 * @param 	jedis
	 * 			jedis
	 */
	public abstract void useFinish(Jedis jedis);
	
	protected ICacheExecutor createSetExecutor() {
		return new SetExecutor();
	}
	
	protected ICacheExecutor createGetExecutor() {
		return new GetExecutor();
	}
	
	@Override
	public void expire(String key, long milliseconds) {
		ICacheUnit cacheUnit = cacheUnits.get(key);
		Jedis jedis = getJedis();
		try {
			if (cacheUnit == null || cacheUnit.getStreamCoder() != null) {
				jedis.pexpire(serializable(key), milliseconds);
			} else {
				jedis.pexpire(key.toString(), milliseconds);
			}
		} catch (Exception e) {
			log.error("error on key " + key, e);
			throw new RedisException(e);
		} finally {
			useFinish(jedis);
		}
	}
	
	@Override
	public boolean exists(String key) {
		return (boolean) existsExecutor.exec(key, null, null, cacheUnits.get(key));
	}
	
	protected class ExistsExecutor extends CacheExecutor {

		@Override
		protected Object execReids(Jedis jedis, String key, String name, String object) throws Exception {
			return jedis.exists(key);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, byte[] name, byte[] object, IStreamCoder streamCoder) throws Exception {
			return jedis.exists(key);
		}

		@Override
		protected Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) throws Exception {
			return jedis.hexists(key, names[0]);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IStreamCoder streamCoder, byte[]... names) throws Exception {
			return jedis.hexists(key, names[0]);
		}
		
	}

	@Override
	public boolean hexists(String key, String name) {
		return (boolean) existsExecutor.exec(key, null, cacheUnits.get(key), null, name);
	}

	@Override
	public void set(String key, Object object) {
		setExecutor.exec(key, null, object, cacheUnits.get(key));
	}
	
	protected abstract class CacheExecutor implements ICacheExecutor {

		@Override
		public Object exec(String key, String name, Object object, ICacheUnit cacheUnit) {
			Jedis jedis = getJedis();
			try {
				IStreamCoder streamCoder;
				if (cacheUnit == null) {
					streamCoder = CacheUnitFactory.defaultStreamCoder;
				} else {
					if (object != null) {
						Preconditions.checkArgument(cacheUnit.getValueClass().equals(object.getClass()), "key's value class is [{}] not [{}].", cacheUnit.getValueClass(), object.getClass());
					}
					streamCoder = cacheUnit.getStreamCoder();
				}
				
				if (streamCoder == null) {
					return execReids(jedis, key.toString(), name == null ? null : name.toString(), object == null ? null : object.toString());
				} else {
					return execReids(jedis, serializable(key), name == null ? null : serializable(name), object == null ? null : streamCoder.write(object), streamCoder);
				}
			} catch (Exception e) {
				log.error("error on key " + key, e);
				throw new RedisException(e);
			} finally {
				useFinish(jedis);
			}
		}
		
		@Override
		public Object exec(String key, Map<String, Object> map, ICacheUnit cacheUnit, Collection<Object> collection, String... names) {
			Jedis jedis = getJedis();
			try {
				IStreamCoder streamCoder = cacheUnit == null ? CacheUnitFactory.defaultStreamCoder : cacheUnit.getStreamCoder();
				
				if (streamCoder == null) {
					Map<String, String> stringMap = map == null ? null : (stringMap = Maps.newHashMap());
					String[] stringNames = new String[names.length];
					try {
						if (map != null) {
							for (Serializable k : map.keySet()) {
								stringMap.put(k.toString(), map.get(k).toString());
							}
						}
						for (int i = 0;i < names.length;i++) {
							stringNames[i] = names[i].toString();
						}
						return execReids(jedis, key == null ? null : key.toString(), stringMap, collection, stringNames);
					} finally {
						if (map != null && map.size() == 0) {
							for (String k : stringMap.keySet()) {
								map.put(k, stringMap.get(k));
							}
						}
					}
				} else {
					Map<byte[], byte[]> byteMap = map == null ? null : (byteMap = Maps.newHashMap());
					byte[][] byteNames = new byte[names.length][];
					try {
						if (map != null) {
							for (Serializable k : map.keySet()) {
								byteMap.put(serializable(k), serializable((Serializable) map.get(k)));
							}
						}
						for (int i = 0;i < names.length;i++) {
							byteNames[i] = serializable(names[i]);
						}
						return execReids(jedis, key == null ? null : serializable(key), byteMap, collection, streamCoder, byteNames);
					} finally {
						if (map != null && map.size() == 0) {
							for (byte[] k : byteMap.keySet()) {
								map.put(deserializable(k).toString(), deserializable(byteMap.get(k)));
							}
						}
					}
				}
			} catch (Exception e) {
				log.error("error on key " + key, e);
				throw new RedisException(e);
			} finally {
				useFinish(jedis);
			}
		}

		protected abstract Object execReids(Jedis jedis, String key, String name, String object) throws Exception;
		
		protected abstract Object execReids(Jedis jedis, byte[] key, byte[] name, byte[] object, IStreamCoder streamCoder) throws Exception;

		protected abstract Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) throws Exception;
		
		protected abstract Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IStreamCoder streamCoder, byte[]... names) throws Exception;
		
	}
	
	protected class SetExecutor extends CacheExecutor {

		@Override
		protected Object execReids(Jedis jedis, String key, String name, String object) {
			return jedis.set(key, object);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, byte[] name, byte[] object, IStreamCoder streamCoder) {
			return jedis.set(key, object);
		}

		@Override
		protected Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) {
			for (String k : map.keySet()) {
				execReids(jedis, k, null, map.get(k));
			}
			return null;
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IStreamCoder streamCoder, byte[]... names) {
			for (byte[] k : map.keySet()) {
				execReids(jedis, k, null, map.get(k), streamCoder);
			}
			return null;
		}
		
	}

	@Override
	public void hset(String key, String name, Object object) {
		hsetExecutor.exec(key, name, object, cacheUnits.get(key));
	}
	
	protected class HsetExecutor extends CacheExecutor {

		@Override
		protected Object execReids(Jedis jedis, String key, String name, String object) {
			return jedis.hset(key, name, object);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, byte[] name, byte[] object, IStreamCoder streamCoder) {
			return jedis.hset(key, name, object);
		}

		@Override
		protected Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) {
			return jedis.hmset(key, map);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IStreamCoder streamCoder, byte[]... names) {
			return jedis.hmset(key, map);
		}
		
	}

	@Override
	public void hmSet(String key, Map<String, Object> map) {
		hsetExecutor.exec(key, map, cacheUnits.get(key), null);
	}

	@Override
	public void del(String key) {
		delExecutor.exec(key, null, null, cacheUnits.get(key));
	}
	
	protected void del(ICacheUnit cacheUnit) {
		delExecutor.exec(cacheUnit.getKey(), null, null, cacheUnit);
	}
	
	protected class DelExecutor extends CacheExecutor {

		@Override
		protected Object execReids(Jedis jedis, String key, String name, String object) {
			return jedis.del(key);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, byte[] name, byte[] object, IStreamCoder streamCoder) {
			return jedis.del(key);
		}

		@Override
		protected Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) {
			for (String k : names) {
				execReids(jedis, k, null, null);
			}
			return null;
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IStreamCoder streamCoder, byte[]... names) {
			for (byte[] k : names) {
				execReids(jedis, k, null, null, streamCoder);
			}
			return null;
		}
		
	}

	@Override
	public Object get(String key) {
		return getExecutor.exec(key, null, null, cacheUnits.get(key));
	}
	
	protected class GetExecutor extends CacheExecutor {

		@Override
		protected Object execReids(Jedis jedis, String key, String name, String object) throws Exception {
			return jedis.get(key);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, byte[] name, byte[] object, IStreamCoder streamCoder) throws Exception {
			return streamCoder.read(jedis.get(key));
		}

		@Override
		protected Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) throws Exception {
			for (String k : names) {
				collection.add(execReids(jedis, k, null, null));
			}
			return null;
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IStreamCoder streamCoder, byte[]... names) throws Exception {
			for (byte[] k : names) {
				collection.add(execReids(jedis, k, null, null, streamCoder));
			}
			return null;
		}
		
	}

	@Override
	public Object hget(String key, String name) {
		return hgetExecutor.exec(key, name, null, cacheUnits.get(key));
	}
	
	protected class HgetExecutor extends CacheExecutor {

		@Override
		protected Object execReids(Jedis jedis, String key, String name, String object) throws Exception {
			return jedis.hget(key, name);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, byte[] name, byte[] object, IStreamCoder streamCoder) throws Exception {
			return streamCoder.read(jedis.hget(key, name));
		}

		@Override
		protected Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) throws Exception {
			collection.addAll(jedis.hmget(key, names));
			return null;
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IStreamCoder streamCoder, byte[]... names) throws Exception {
			for (byte[] datas : jedis.hmget(key, names)) {
				collection.add(streamCoder.read(datas));
			}
			return null;
		}
		
	}

	@Override
	public List<Object> hmGet(String key, String... names) {
		List<Object> list = Lists.newArrayListWithCapacity(names.length);
		hgetExecutor.exec(key, null, cacheUnits.get(key), list, names);
		return list;
	}

	@Override
	public void hdel(String key, String name) {
		hdelExecutor.exec(key, name, null, cacheUnits.get(key));
	}
	
	protected class HdelExecutor extends CacheExecutor {

		@Override
		protected Object execReids(Jedis jedis, String key, String name, String object) throws Exception {
			return jedis.hdel(key, name);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, byte[] name, byte[] object, IStreamCoder streamCoder) throws Exception {
			return jedis.hdel(key, name);
		}

		@Override
		protected Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) throws Exception {
			return names.length > 0 ? jedis.hdel(key, names) : 0L;
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IStreamCoder streamCoder, byte[]... names) throws Exception {
			return names.length > 0 ? jedis.hdel(key, names) : 0L;
		}
		
	}

	@Override
	public void hmDel(String key, String... names) {
		hdelExecutor.exec(key, null, cacheUnits.get(key), Lists.newArrayListWithCapacity(names.length), names);
	}

	@Override
	public long hlen(String key) {
		return (long) hlenAndHgetExecutor.exec(key, null, null, cacheUnits.get(key));
	}
	
	protected class HlenAndHgetAllExecutor extends CacheExecutor {

		@Override
		protected Object execReids(Jedis jedis, String key, String name, String object) throws Exception {
			return jedis.hlen(key);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, byte[] name, byte[] object, IStreamCoder streamCoder) throws Exception {
			return jedis.hlen(key);
		}

		@Override
		protected Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) throws Exception {
			map.putAll(jedis.hgetAll(key));
			return null;
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IStreamCoder streamCoder, byte[]... names) throws Exception {
			map.putAll(jedis.hgetAll(key));
			return null;
		}
		
	}

	@Override
	public Map<String, Object> hGetAll(String key) {
		Map<String, Object> map = Maps.newHashMap();
		hlenAndHgetExecutor.exec(key, map, cacheUnits.get(key), null);
		return map;
	}

	@Override
	public Set<Object> hKeys(String key) {
		Set<Object> keys = Sets.newHashSet();
		hkeysAndTtlExecutor.exec(key, null, cacheUnits.get(keys), keys);
		return keys;
	}
	
	protected class HkeysAndTtlExecutor extends CacheExecutor {

		@Override
		protected Object execReids(Jedis jedis, String key, String name, String object) throws Exception {
			return jedis instanceof Jedis ? ((Jedis) jedis).pttl(key) : jedis.ttl(key);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, byte[] name, byte[] object, IStreamCoder streamCoder) throws Exception {
			return execReids(getJedis(), deserializable(key).toString(), null, null);
		}

		@Override
		protected Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) throws Exception {
			return collection.addAll(jedis.hkeys(key));
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IStreamCoder streamCoder, byte[]... names) throws Exception {
			for (byte[] k : jedis.hkeys(key)) {
				collection.add(streamCoder.read(k));
			}
			return null;
		}
		
	}
	
	@Override
	public String set(String key, String value, String nxxx, String expx, long time) {
		Jedis jedis = getJedis();
		try {
			return jedis.set(key, value, nxxx, expx, time);
		} catch(Exception e) {
			log.error("error on key " + key, e);
			throw new RedisException(e);
		} finally {
			useFinish(jedis);
		}
	}
	
	@Override
	public long ttl(String key) {
		return (long) hkeysAndTtlExecutor.exec(key, null, null, cacheUnits.get(key));
	}
	
	@Override
	public String type(String key) {
		return (String) typeExecutor.exec(key, null, null, cacheUnits.get(key));
	}
	
	protected class TypeExecutor extends CacheExecutor {

		@Override
		protected Object execReids(Jedis jedis, String key, String name, String object) throws Exception {
			return jedis.type(key);
		}

		@Override
		protected Object execReids(Jedis jedis, byte[] key, byte[] name, byte[] object, IStreamCoder streamCoder) throws Exception {
			return jedis.type(key);
		}

		@Deprecated
		@Override
		protected Object execReids(Jedis jedis, String key, Map<String, String> map, Collection<Object> collection, String... names) throws Exception {
			return null;
		}

		@Deprecated
		@Override
		protected Object execReids(Jedis jedis, byte[] key, Map<byte[], byte[]> map, Collection<Object> collection, IStreamCoder streamCoder, byte[]... names) throws Exception {
			return null;
		}
		
	}
	
	/**
	 * 序列化
	 * @param 	object
	 * 			被序列化的对象
	 * @return	序列化结果
	 * @throws 	Exception
	 */
	protected static byte[] serializable(Serializable object) throws Exception {
		return SerializaUtil.serializable(object);
	}
	
	/**
	 * 反序列化
	 * @param 	datas
	 * 			序列化内容
	 * @return	反序列化的对象
	 * @throws 	Exception
	 */
	protected static Serializable deserializable(byte[] datas) throws Exception {
		return SerializaUtil.deserializable(datas);
	}
	
	@Override
	public void registerCache(String key, @SuppressWarnings("rawtypes") Class valueClass, boolean delAtShutdown, IStreamCoder streamCoder) {
		Preconditions.checkArgument(!cacheUnits.containsKey(key), "Repeat cache key[{}].", key);
		cacheUnits.put(key, CacheUnitFactory.createCacheUnit(key, valueClass, delAtShutdown, streamCoder));
	}
	
	@Override
	public void shutdown() {
		for (ICacheUnit cacheUnit : cacheUnits.values()) {
			if (cacheUnit.delAtShutdown()) {
				del(cacheUnit);
				log.info("Delete key [{}] on shutdown.", cacheUnit.getKey());
			}
		}
	}
	
	protected ICacheUnit checkMCache(String... keys) {
		ICacheUnit cacheUnit = null;
		for (String key : keys) {
			if (!(cacheUnit == null && !cacheUnits.containsKey(key))) {
				Preconditions.checkArgument(cacheUnit != null && cacheUnits.containsKey(key), "Not only cache unit.");
			}
			cacheUnit = cacheUnits.get(key);
		}
		return cacheUnit;
	}

}
