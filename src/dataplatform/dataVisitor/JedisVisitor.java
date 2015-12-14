package dataplatform.dataVisitor;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import dataplatform.coder.bytes.ByteCoders;
import dataplatform.coder.bytes.IBytesCoder;
import dataplatform.coder.string.IStringCoder;
import dataplatform.coder.string.StringCoders;
import redis.clients.jedis.Jedis;

public abstract class JedisVisitor implements IDataVisitor {
	
	protected static final Logger log = LoggerFactory.getLogger(JedisVisitor.class);
	
	public static final String CONDITION_KEY = "key";
	
	public static final String CONDITION_NAME = "name";
	
	@SuppressWarnings("rawtypes")
	private static final Map<Class, IStringCoder> STRING_CODERS;
	
	private static final IBytesCoder BYTES_CODES;
	
	static {
		STRING_CODERS = Maps.newHashMap();
		STRING_CODERS.put(Integer.class, StringCoders.newIntegerStringCoder());
		STRING_CODERS.put(Byte.class, StringCoders.newByteStringCoder());
		STRING_CODERS.put(Double.class, StringCoders.newDoubleStringCoder());
		STRING_CODERS.put(Float.class, StringCoders.newFloatStringCoder());
		STRING_CODERS.put(Long.class, StringCoders.newLongStringCoder());
		STRING_CODERS.put(Short.class, StringCoders.newShortStringCoder());
		BYTES_CODES = ByteCoders.newSerialableCoder();
	}
	
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

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(Class<T> clz, EntityType entityType, Map<String, Object> conditions) {
		Jedis jedis = getJedis();
		String key = (String) Preconditions.checkNotNull(conditions.get(CONDITION_KEY), "Do not have condition : {}", CONDITION_KEY);
		String name = (String) conditions.get(CONDITION_NAME);
		if (isString(clz)) {
			String value;
			if (name == null || name.length() == 0) {
				value = jedis.get(key);
			} else {
				value = jedis.hget(key, name);
			}
			return (T) castFromString(clz, value);
		} else {
			byte[] bytes;
			try {
				if (name == null || name.length() == 0) {
					bytes = jedis.get(stringToBytes(key));
				} else {
					bytes = jedis.hget(stringToBytes(key), stringToBytes(name));
				}
				return (T) BYTES_CODES.read(bytes);
			} catch (Exception e) {
				throw new RuntimeException(MessageFormat.format("JedisVisitor get/hget error on key / name : {} / {} ", key, name));
			}
		}
	}
	
	private <T> boolean isString(Class<T> clz) {
		for (@SuppressWarnings("rawtypes") Class isStringClass : STRING_CODERS.keySet()) {
			if (clz.equals(isStringClass)) {
				return true;
			}
		}
		return false;
	}
	
	private <T> Object castFromString(Class<T> clz, String value) {
		return STRING_CODERS.containsKey(clz) ? STRING_CODERS.get(clz).parse(value) : value.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> getList(Class<T> clz, EntityType entityType, Map<String, Object> conditions) {
		Jedis jedis = getJedis();
		List<T> list = Lists.newLinkedList();
		Object conditionKey = Preconditions.checkNotNull(conditions.get(CONDITION_KEY), "Do not have condition : {}", CONDITION_KEY);
		String[] names = (String[]) conditions.get(CONDITION_NAME);
		if (isString(clz)) {
			if (conditionKey instanceof String[]) {
				jedis.mget((String[]) conditionKey).forEach(value -> list.add((T) value));
			} else if (names == null) {
				jedis.hgetAll(conditionKey.toString()).values().forEach(value -> list.add((T) value));
			} else {
				jedis.hmget(conditionKey.toString(), names).forEach(value -> list.add((T) value));
			}
		} else {
			if (conditionKey instanceof String[]) {
				String[] keys = (String[]) conditionKey;
				byte[][] keyBytes = stringsToBytes(keys);
				jedis.mget(keyBytes).forEach((value) -> {
					try {
						list.add((T) BYTES_CODES.read(value));
					} catch (Exception e) {
						StringBuilder builder = new StringBuilder("JedisVisitor mget error on key : ");
						for (String key : keys) {
							builder.append(key).append(" ; ");
						}
						throw new RuntimeException(builder.toString());
					}
				});
			} else if (names == null) {
				jedis.hgetAll(stringToBytes(conditionKey.toString())).values().forEach((value) -> {
					try {
						list.add((T) BYTES_CODES.read(value));
					} catch (Exception e) {
						throw new RuntimeException(MessageFormat.format("JedisVisitor hgetAll error on key : {} ", conditionKey));
					}
				});
			} else {
				byte[][] nameBytes = stringsToBytes(names);
				jedis.hmget(stringToBytes(conditionKey.toString()), nameBytes).forEach((value) -> {
					try {
						list.add((T) BYTES_CODES.read(value));
					} catch (Exception e) {
						StringBuilder builder = new StringBuilder("JedisVisitor hmget error on key : ");
						builder.append(conditionKey).append(" / ");
						for (String name : names) {
							builder.append(name).append(" ; ");
						}
						throw new RuntimeException(builder.toString());
					}
				});
			}
		}
		return list;
	}
	
	private static byte[] stringToBytes(String str) {
		return str.getBytes();
	}
	
	private static byte[][] stringsToBytes(String[] strs) {
		byte[][] bytes = new byte[strs.length][];
		for (int i = 0;i < strs.length;i++) {
			bytes[i] = stringToBytes(strs[i]);
		}
		return bytes;
	}

	@Override
	public <T> void save(T entity, EntityType entityType, Map<String, Object> conditions) {
		Jedis jedis = getJedis();
		String key = (String) Preconditions.checkNotNull(conditions.get(CONDITION_KEY), "Do not have condition : {}", CONDITION_KEY);
		String name = (String) conditions.get(CONDITION_NAME);
		if (isString(entity.getClass())) {
			if (name == null || name.length() == 0) {
				jedis.set(key, entity.toString());
			} else {
				jedis.hset(key, name, entity.toString());
			}
		} else {
			try {
				if (name == null || name.length() == 0) {
					jedis.set(stringToBytes(key), BYTES_CODES.write(entity));
				} else {
					jedis.hset(stringToBytes(key), stringToBytes(name), BYTES_CODES.write(entity));
				}
			} catch (Exception e) {
				throw new RuntimeException(MessageFormat.format("JedisVisitor set/hset error on key / name : {} / {} ", key, name));
			}
		}
	}

	@Override
	public <T> void delete(T entity, EntityType entityType, Map<String, Object> conditions) {
		Jedis jedis = getJedis();
		String key = (String) Preconditions.checkNotNull(conditions.get(CONDITION_KEY), "Do not have condition : {}", CONDITION_KEY);
		String name = (String) conditions.get(CONDITION_NAME);
		if (isString(entity.getClass())) {
			if (name == null || name.length() == 0) {
				jedis.del(key, entity.toString());
			} else {
				jedis.hdel(key, name, entity.toString());
			}
		} else {
			try {
				if (name == null || name.length() == 0) {
					jedis.del(stringToBytes(key), BYTES_CODES.write(entity));
				} else {
					jedis.hdel(stringToBytes(key), stringToBytes(name), BYTES_CODES.write(entity));
				}
			} catch (Exception e) {
				throw new RuntimeException(MessageFormat.format("JedisVisitor del/hdel error on key / name : {} / {} ", key, name));
			}
		}
	}

	@Override
	public <T> void save(T[] entity, EntityType entityType, Map<String, Object> conditions) {
		if (entity.length > 0) {
			Jedis jedis = getJedis();
			Object conditionKey = Preconditions.checkNotNull(conditions.get(CONDITION_KEY), "Do not have condition : {}", CONDITION_KEY);
			Object conditionNames = conditions.get(CONDITION_NAME);
			if (isString(entity[0].getClass())) {
				if (conditionKey instanceof String[]) {
					String[] keys = (String[]) conditionKey;
					int length = entity.length;
					String[] keysvalues = new String[length << 1];
					for (int i = 0;i < length;i++) {
						keysvalues[i] = keys[i];
						keysvalues[i + length] = entity[i].toString();
					}
					jedis.mset(keysvalues);
				} else if (conditionKey instanceof String) {
					if (conditionNames instanceof String[]) {
						Map<String, String> map = Maps.newHashMap();
						String[] names = (String[]) conditionNames;
						for (int i = 0;i < entity.length;i++) {
							map.put(names[i], entity[i].toString());
						}
						jedis.hmset(conditionKey.toString(), map);
					} else {
						throw new RuntimeException(MessageFormat.format("JedisVisitor save error on key : {}", conditionKey));
					}
				} else {
					throw new RuntimeException(MessageFormat.format("JedisVisitor save error on key : {}", conditionKey));
				}
			} else {
				try {
					if (conditionKey instanceof String[]) {
						String[] keys = (String[]) conditionKey;
						int length = entity.length;
						byte[][] keysvalues = new byte[length << 1][];
						for (int i = 0;i < length;i++) {
							keysvalues[i] = stringToBytes(keys[i]);
							keysvalues[i + length] = BYTES_CODES.write(entity[i]);
						}
						jedis.mset(keysvalues);
					} else if (conditionKey instanceof String) {
						if (conditionNames instanceof String[]) {
							Map<byte[], byte[]> map = Maps.newHashMap();
							String[] names = (String[]) conditionNames;
							for (int i = 0;i < entity.length;i++) {
								map.put(stringToBytes(names[i]), BYTES_CODES.write(entity[i]));
							}
							jedis.hmset(stringToBytes(conditionKey.toString()), map);
						} else {
							throw new RuntimeException(MessageFormat.format("JedisVisitor save error on key : {}", conditionKey));
						}
					} else {
						throw new RuntimeException(MessageFormat.format("JedisVisitor save error on key : {}", conditionKey));
					}
				} catch (Exception e) {
					throw new RuntimeException(MessageFormat.format("JedisVisitor mset/hmset error on key / name : {} / {} ", conditionKey, conditionNames));
				}
			}
		}
	}

	@Override
	public <T> void delete(T[] entity, EntityType entityType, Map<String, Object> conditions) {
		Object conditionKey = Preconditions.checkNotNull(conditions.get(CONDITION_KEY), "Do not have condition : {}", CONDITION_KEY);
		Object conditionNames = conditions.get(CONDITION_NAME);
		if (conditionKey instanceof String[]) {
			String[] keys = (String[]) conditionKey;
			Map<String, Object> map = Maps.newHashMap();
			for (int i = 0;i < entity.length;i++) {
				map.put(CONDITION_KEY, keys[i]);
				delete(entity[i], entityType, map);
				map.clear();
			}
		} else {
			String key = conditionKey.toString();
			String[] names = (String[]) conditionNames;
			Map<String, Object> map = Maps.newHashMap();
			for (int i = 0;i < entity.length;i++) {
				map.put(CONDITION_KEY, key);
				map.put(CONDITION_NAME, names[i]);
				delete(entity[i], entityType, map);
				map.clear();
			}
		}
	}

}
