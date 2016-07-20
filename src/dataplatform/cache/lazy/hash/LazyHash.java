package dataplatform.cache.lazy.hash;

import java.util.Map;
import java.util.Set;

import dataplatform.cache.lazy.ILazyCache;
import dataplatform.cache.manager.ICachePlatform;

public class LazyHash<F, V> implements ILazyHash<F, V> {
	
	protected final ILazyCache<F, V> cache;
	
	protected final Class<V> vclz;
	
	public LazyHash(ICachePlatform cachePlatform, Class<F> fclz, Class<V> vclz, String preKey) {
		this.vclz = vclz;
		cache = cachePlatform.createLazyCache(fclz, vclz, preKey);
	}

	@Override
	public void hashSet(F field, V value) {
		nativeHashSet(field, value);
	}
	
	protected void nativeHashSet(F field, V value) {
		cache.hash().set(field, value);
	}

	@Override
	public V hashGet(F field) {
		return cache.hash().get(field);
	}

	@Override
	public void hashDelete(F field) {
		cache.hash().remove(field);
	}

	@Override
	public Map<F, V> getAll() {
		return cache.hash().getAll();
	}

	@Override
	public int hashSize() {
		return (int) cache.hash().size();
	}

	@Override
	public Class<V> getValueClass() {
		return vclz;
	}

	@Override
	public void clear() {
		cache.key().delete();
	}

	@Override
	public void hashSet(Map<F, V> values) {
		cache.hash().set(values);
	}

	@Override
	public Set<F> fields() {
		return cache.hash().fields();
	}

}
