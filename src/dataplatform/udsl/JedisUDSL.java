package dataplatform.udsl;

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

public abstract class JedisUDSL implements UDSL {
	
	protected static final Logger log = LoggerFactory.getLogger(JedisUDSL.class);
	
	public static final String CONDITION_NAME = "name";
	
	@SuppressWarnings("rawtypes")
	private static final Map<Class, IStringCoder> STRING_CODERS;
	
	private static final IBytesCoder BYTES_CODES;
	
	private final ICachedObjects cachedObject;
	
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
	
	public JedisUDSL(ICachedObjects cachedObject) {
		this.cachedObject = cachedObject;
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
	
	private static <T> boolean isString(Class<T> clz) {
		for (@SuppressWarnings("rawtypes") Class isStringClass : STRING_CODERS.keySet()) {
			if (clz.equals(isStringClass)) {
				return true;
			}
		}
		return false;
	}
	
	private static <T> Object castFromString(Class<T> clz, String value) {
		return STRING_CODERS.containsKey(clz) ? STRING_CODERS.get(clz).parse(value) : value.toString();
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

	@SuppressWarnings("unchecked")
	@Override
	public <T> T fetch(Object... params) {
		Jedis jedis = getJedis();
		String key = (String) Preconditions.checkNotNull(params[0], "Do not have condition : {}", params[0]);
		String name = (String) params[1];
		Class<T> clz = (Class<T>) params[2];
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

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> find(Object... params) {
		Jedis jedis = getJedis();
		List<T> list = Lists.newLinkedList();
		Object conditionKey = Preconditions.checkNotNull(params[0], "Do not have condition : {}", params[0]);
		String[] names = (String[]) params[1];
		Class<T> clz = (Class<T>) params[2];
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

	@Deprecated
	@Override
	public <T> List<T> orderBy(boolean dec, Object... params) {
		return null;
	}

	@Deprecated
	@Override
	public <T> List<T> limit(int offset, int count, Object... params) {
		return null;
	}

	@Override
	public <T> void save(T entity) {
		Jedis jedis = getJedis();
		String key = cachedObject.makeKey(entity);
		String name = cachedObject.makeField(entity);
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
	public <T> void delete(T entity) {
		Jedis jedis = getJedis();
		String key = cachedObject.makeKey(entity);
		String name = cachedObject.makeField(entity);
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

}