package dataplatform.cache.redis;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import dataplatform.cache.ICache;
import dataplatform.util.SerializaUtil;

public abstract class CacheOnJedis<B extends BinaryJedisCommands, J extends JedisCommands> implements ICache {
	
	protected static final Logger log = LoggerFactory.getLogger(CacheOnJedis.class);
	
	/**
	 * 获取二进制Redis命令客户端
	 * @return	二进制Redis命令客户端
	 */
	public abstract B getBinaryJedisCommands();
	/**
	 * 使用结束处理
	 * @param 	jedis
	 * 			redis客户端
	 */
	public abstract void useFinish(B jedis);
	/**
	 * 获取字符串Redis命令客户端
	 * @return	字符串Redis命令客户端
	 */
	public abstract J getJedisCommands();
	/**
	 * 使用结束处理
	 * @param 	jedis
	 * 			redis客户端
	 */
	public abstract void useFinish(J jedis);
	
	@Override
	public boolean exists(Serializable key) {
		B jedis = getBinaryJedisCommands();
		try {
			return jedis.exists(serializable(key));
		} catch (Exception e) {
			log.error("error on key " + key, e);
			return false;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public boolean hexists(Serializable key, Serializable name) {
		B jedis = getBinaryJedisCommands();
		try {
			return jedis.hexists(serializable(key), serializable(name));
		} catch (Exception e) {
			log.error("error on key " + key, e);
			return false;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void set(Serializable key, Serializable object) {
		B jedis = getBinaryJedisCommands();
		try {
			jedis.set(serializable(key), serializable(object));
		} catch (Exception e) {
			log.error("error on key " + key, e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void hset(Serializable key, Serializable name, Serializable object) {
		B jedis = getBinaryJedisCommands();
		try {
			jedis.hset(serializable(key), serializable(name), serializable(object));
		} catch (Exception e) {
			log.error("error on key " + key, e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void hmSet(Serializable key, Map<Serializable, Serializable> map) {
		B jedis = getBinaryJedisCommands();
		try {
			Map<byte[], byte[]> byteMap = Maps.newHashMap();
			for (Serializable k : map.keySet()) {
				byteMap.put(serializable(k), serializable(map.get(k)));
			}
			jedis.hmset(serializable(key), byteMap);
		} catch (Exception e) {
			log.error("error on key " + key, e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void del(Serializable key) {
		B jedis = getBinaryJedisCommands();
		try {
			jedis.del(serializable(key));
		} catch (Exception e) {
			log.error("error on key " + key, e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public Serializable get(Serializable key) {
		B jedis = getBinaryJedisCommands();
		try {
			return deserializable(jedis.get(serializable(key)));
		} catch (Exception e) {
			log.error("error on key " + key, e);
			return null;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public Serializable hget(Serializable key, Serializable name) {
		B jedis = getBinaryJedisCommands();
		try {
			return deserializable(jedis.hget(serializable(key), serializable(name)));
		} catch (Exception e) {
			log.error("error on key " + key, e);
			return null;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public List<Serializable> hmGet(Serializable key, Serializable... names) {
		B jedis = getBinaryJedisCommands();
		try {
			byte[][] nameBytes = serializable(names);
			List<byte[]> list = jedis.hmget(serializable(key), nameBytes);
			return deserializable(list);
		} catch (Exception e) {
			log.error("error on key " + key, e);
			return Lists.newArrayList();
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void hdel(Serializable key, Serializable name) {
		B jedis = getBinaryJedisCommands();
		try {
			jedis.hdel(serializable(key), serializable(name));
		} catch (Exception e) {
			log.error("error on key " + key, e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public void hmDel(Serializable key, Serializable... names) {
		B jedis = getBinaryJedisCommands();
		try {
			byte[][] nameBytes = serializable(names);
			for (byte[] name : nameBytes) {
				jedis.hdel(serializable(key), name);
			}
		} catch (Exception e) {
			log.error("error on key " + key, e);
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public long hlen(Serializable key) {
		B jedis = getBinaryJedisCommands();
		try {
			return jedis.hlen(serializable(key));
		} catch (Exception e) {
			log.error("error on key " + key, e);
			return 0;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public Map<Serializable, Serializable> hGetAll(Serializable key) {
		B jedis = getBinaryJedisCommands();
		try {
			Map<byte[], byte[]> all = jedis.hgetAll(serializable(key));
			Map<Serializable, Serializable> ret = Maps.newHashMap();
			for (byte[] name : all.keySet()) {
				ret.put(deserializable(name), deserializable(all.get(name)));
			}
			return ret;
		} catch (Exception e) {
			log.error("error on key " + key, e);
			return null;
		} finally {
			useFinish(jedis);
		}
	}

	@Override
	public Set<Serializable> hKeys(Serializable key) {
		B jedis = getBinaryJedisCommands();
		try {
			Set<byte[]> bytes = jedis.hkeys(serializable(key));
			Set<Serializable> keys = Sets.newHashSet();
			for (byte[] data : bytes) {
				keys.add(deserializable(data));
			}
			return keys;
		} catch (Exception e) {
			log.error("error on key " + key, e);
			return null;
		} finally {
			useFinish(jedis);
		}
	}
	
	@Override
	public String set(String key, String value, String nxxx, String expx, long time) {
		J jedis = getJedisCommands();
		try {
			return jedis.set(key, value, nxxx, expx, time);
		} catch(Exception e) {
			log.error("error on key " + key, e);
			return null;
		} finally {
			useFinish(jedis);
		}
	}
	
	@Override
	public long ttl(String key) {
		J jedis = getJedisCommands();
		try {
			log.info((jedis instanceof Jedis ? ((Jedis) jedis).pttl(key) : jedis.ttl(key)) + " : " + jedis.getClass());
			return jedis instanceof Jedis ? ((Jedis) jedis).pttl(key) : jedis.ttl(key);
		} catch (Exception e) {
			log.error("error on key " + key, e);
			return 0;
		} finally {
			useFinish(jedis);
		}
	}
	
	@Override
	public void del(String key) {
		J jedis = getJedisCommands();
		try {
			jedis.del(key);
		} catch (Exception e) {
			log.error("error on key " + key, e);
		} finally {
			useFinish(jedis);
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
	
	/**
	 * 序列化
	 * @param 	objects
	 * 			被序列化的对象集合
	 * @return	序列化结果
	 * @throws 	Exception
	 */
	protected static byte[][] serializable(Serializable... objects) throws Exception {
		return SerializaUtil.serializable(objects);
	}
	
	/**
	 * 反序列化
	 * @param 	list
	 * 			序列化内容集合
	 * @return	反序列化的内容列表
	 * @throws 	Exception
	 */
	protected static List<Serializable> deserializable(List<byte[]> list) throws Exception {
		return SerializaUtil.deserializable(list);
	}

}
