package com.lufax.dubbo.registry.db;

import static org.junit.Assert.*;

import javax.sql.DataSource;

import org.junit.Test;

import com.alibaba.dubbo.common.URL;

public class RegistryDBStoreTest extends RegistryDBStore {

	@Test
	public void testAll() {
		org.h2.jdbcx.JdbcDataSource ds = new org.h2.jdbcx.JdbcDataSource();
		ds.setURL("jdbc:h2:~/.dubbo/h2");
		RegistryDBStore store = new RegistryDBStore();
		store.setDataSource(ds);
		store.remove(2L);
		System.out.println(store.all());
		long id = store.add(URL.valueOf("test://localhost:999/xpath?param1=value1&param2=value2"));
		System.out.println(store.all());
		store.remove(id);
		System.out.println(store.all());
	}

}
