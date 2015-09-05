package cache;

import java.io.Serializable;

/**
 * 缓存键容器
 * @author 	fuhuiyuan
 */
public interface ICacheKeyContainer {
	
	/**
	 * 获得缓存键组
	 * @return	缓存键组
	 */
	Serializable[] getKeys();

}
