package dataplatform.cache;

import java.io.Serializable;

/**
 * 带有前缀的缓存键容器
 * @author 	fuhuiyuan
 */
public abstract class PreKeyContainer<T extends ICache> implements ICacheKeyContainer {
	
	/**键前缀*/
	protected final Serializable preKey;
	/**缓存器*/
	protected final T cache;
	
	public PreKeyContainer(Serializable preKey, T cache) {
		this.preKey = preKey;
		this.cache = cache;
	}

}
