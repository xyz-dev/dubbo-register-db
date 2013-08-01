package com.lufax.dubbo.registry.db;

import java.util.List;

import com.alibaba.dubbo.common.URL;

public interface RegistryStore {

	public void add(URL url);
	
	public void remove(URL url);
	
	public List<URL> all();
}
