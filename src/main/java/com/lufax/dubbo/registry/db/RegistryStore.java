package com.lufax.dubbo.registry.db;

import java.util.Map;

import com.alibaba.dubbo.common.URL;

public interface RegistryStore {

	public Long add(URL url);

	public void remove(Long id);

	public Map<Long, URL> all();
}
