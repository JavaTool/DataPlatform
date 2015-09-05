package dataplatform.cache;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 缓存清理器
 * @author 	fuhuiyuan
 */
public class CacheCleaner {
	
	protected static final Logger log = LoggerFactory.getLogger(CacheCleaner.class);
	/**缓存键容器组*/
	protected final List<ICacheKeyContainer> containers;
	/**缓存器*/
	protected final ICache cache;
	
	public CacheCleaner(ICache cache) {
		this.cache = cache;
		containers = createContainers();
	}
	
	/**
	 * 创建缓存键容器组
	 * @return	缓存键容器组
	 */
	protected List<ICacheKeyContainer> createContainers() {
		return new LinkedList<ICacheKeyContainer>();
	}
	
	/**
	 * 添加缓存键容器
	 * @param 	container
	 * 			缓存键容器
	 */
	public void addContainer(ICacheKeyContainer container) {
		containers.add(container);
	}
	
	/**
	 * 清空缓存
	 */
	public void clear() {
		for (ICacheKeyContainer cacheKeyContainer : containers) {
			Serializable[] keys = cacheKeyContainer.getKeys();
			if (keys != null) {
				for (Serializable key : keys) {
					cache.del(key);
					log.info("Cache delete key : {}.", key);
				}
			}
		}
	}

}
