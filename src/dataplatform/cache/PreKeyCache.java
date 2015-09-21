package dataplatform.cache;

/**
 * 带前缀键的缓存器
 * @author 	fuhuiyuan
 */
public abstract class PreKeyCache extends PreKeyContainer<IScheduledCache> {
	
	/**创建-键*/
	protected final String createKey;
	/**更新-键*/
	protected final String updateKey;
	/**删除-键*/
	protected final String deleteKey;

	public PreKeyCache(String preKey, IScheduledCache cache, String createKey, String updateKey, String deleteKey) {
		super(preKey, cache);
		this.createKey = preKey + "_" + createKey;
		this.updateKey = preKey + "_" + updateKey;
		this.deleteKey = preKey + "_" + deleteKey;
	}
	
	/**
	 * 创建
	 * @param 	name
	 * 			名称
	 * @param 	value
	 * 			值
	 */
	protected void create(String name, Object value) {
		cache.addHScheduledUpdate(createKey, name, value, true);
	}
	
	/**
	 * 更新
	 * @param 	name
	 * 			名称
	 * @param 	value
	 * 			值
	 */
	protected void update(String name, Object value) {
		cache.addHScheduledUpdate(updateKey, name, value, true);
	}
	
	/**
	 * 删除
	 * @param 	name
	 * 			名称
	 * @param 	value
	 * 			值
	 */
	protected void delete(String name, Object value) {
		cache.addHScheduledDelete(deleteKey, name, value);
	}

}
