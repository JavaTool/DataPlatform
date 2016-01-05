package dataplatform.cache.sequence.impl;

import java.util.Map;

import com.google.common.collect.Maps;

import dataplatform.sequence.IInstanceIdMaker;
import dataplatform.sequence.IInstanceIdManager;

public class RuntimeInstanceIdManager implements IInstanceIdManager {
	
	protected final Map<String, IInstanceIdMaker> makers;
	
	public RuntimeInstanceIdManager() {
		makers = Maps.newHashMap();
	}

	@Override
	public void create(String name, int baseValue) {
		if (!makers.containsKey(name)) {
			IInstanceIdMaker maker = new JavaAutoIdMaker(baseValue);
			makers.put(name, maker);
		}
	}

	@Override
	public int next(String name) {
		return makers.get(name).nextInstanceId();
	}

}
