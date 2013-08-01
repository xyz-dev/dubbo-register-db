package com.lufax.dubbo.registry.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.alibaba.dubbo.common.URL;

public class RegistryDBStore implements RegistryStore {

	private DataSource dataSource;

	private JdbcTemplate jdbc;

	public void add(URL url) {
		jdbc.execute("", new PreparedStatementCallback() {
			public Object doInPreparedStatement(PreparedStatement stmt)
					throws SQLException, DataAccessException {
				return null;
			}

		});
	}

	public void remove(URL url) {
	}

	public static class URLResultSetExtractor implements ResultSetExtractor {
		public Object extractData(ResultSet rs) throws SQLException,
				DataAccessException {
			return null;
		}
	}

	public List<URL> all() {
		jdbc.query("", new URLResultSetExtractor());
		return null;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
		jdbc = new JdbcTemplate(dataSource);
	}

	private void ensureTable() {
		try {
			jdbc.query("select * from "+TABLE+" where 1<>1", new URLResultSetExtractor());
		} catch (DataAccessException ex) {
			jdbc.execute(CREATE_SQL);
		}
	}

	private final String TABLE = "Service_Registry";
	private final String CREATE_SQL = "create table " + TABLE + " (" + "" + ""
			+ "id number(15,0) not null,"
			+ "status varchar(1) not null,"
			+ "url varchar(2000) not null,"
			+ "host varchar(60) not null,"
			+ "port number(8,0) not null,"
			+ "protocol varchar(32),"
			+ "path varchar(255),"
			// parameters
			+ "side varchar(60),"
			+ "timestamp varchar(60),"
			+ "pid varchar(60),"

			+ "application varchar(60),"
			+ "owner varchar(127),"
			+ "interface varchar(255),"
			+ "methods varchar(1000),"
			
			+ "dubbo varchar(60)"
			 + ");";

}
