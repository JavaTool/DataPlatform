package dataplatform.cache.object.hash;

import static dataplatform.util.DateUtil.getMilliSecondStartOfDayRefresh;

import dataplatform.cache.manager.ICacheManager;

public class DayRefreshObjectHash<K, F, V> extends ExpireObjectHash<K, F, V> {

	public DayRefreshObjectHash(ICacheManager cacheManager, Class<F> fclz, Class<V> vclz, String preKey) {
		super(cacheManager, fclz, vclz, preKey);
	}

	@Override
	protected long getExpireTime(V value) {
		return getMilliSecondStartOfDayRefresh();
	}

}
