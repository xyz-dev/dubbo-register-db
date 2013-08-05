package com.lufax.dubbo.registry.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import com.alibaba.dubbo.common.URL;

public class RegistryDBStore implements RegistryStore {

	private DataSource dataSource;
	private transient AtomicLong last;

	private NamedParameterJdbcTemplate jdbc;

	public Long add(URL url) {
		long id = nextId();
		Map<String, Object> params = paramMap(url);
		params.put("id", id);
		jdbc.update(INSERT_SQL, params);
		return id;
	}

	public void remove(Long id) {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("id", id);
		jdbc.update(REMOVE_SQL, params);
	}

	@SuppressWarnings("unchecked")
	public Map<Long, URL> all() {
		Load loader = new Load();
		jdbc.query(ALL_SQL, new HashMap<String, String>(1), loader);
		return loader.results;
	}

	private static class Load implements ResultSetExtractor {
		public Map<Long, URL> results = new HashMap<Long, URL>(100);

		public Object extractData(ResultSet rs) throws SQLException,
				DataAccessException {
			while (rs.next()) {
				Long id = rs.getLong("ID");
				URL url = URL.valueOf(rs.getString("URL"));
				results.put(id, url);
			}
			return null;
		}
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
		jdbc = new NamedParameterJdbcTemplate(dataSource);
		ensureTable();
	}

	private long nextId() {
		return last.incrementAndGet();
	}

	private void ensureTable() {
		try {
			long max = jdbc.queryForLong("select max(id) from " + TABLE,
					new HashMap<String, String>(1));
			this.last = new AtomicLong(max + 1);
		} catch (DataAccessException ex) {
			jdbc.execute(CREATE_SQL, new HashMap<String, String>(1),
					new PreparedStatementCallback() {
						public Object doInPreparedStatement(
								PreparedStatement stmt) throws SQLException,
								DataAccessException {
							return null;
						}

					});
			this.last = new AtomicLong(1);
		}
	}

	private static final String TABLE = "Service_Registry";
	private static String[] columnNames = ("id, status, url, host, port, protocol, path, "
			+ "side, timestamp, pid, application, owner, interface, methods, dubbo")
			.split(",\\s?");

	private static String columns(boolean isParams) {
		StringBuffer sb = new StringBuffer(columnNames.length * 10);
		for (int i = 0; i < columnNames.length; i++) {
			if (i != 0) {
				sb.append(", ");
			}
			if (isParams) {
				sb.append(":");
			}
			sb.append(columnNames[i]);
		}
		return sb.toString();
	}

	private static final String ALL_SQL = "SELECT " + columns(false) + " FROM "
			+ TABLE + " WHERE status=1";

	// private static final String GET_SQL = "SELECT " + columns(false) +
	// " FROM "
	// + TABLE + " WHERE id=:id";

	private static final String REMOVE_SQL = "UPDATE " + TABLE
			+ " SET status=:status WHERE id=:id";

	private static final String INSERT_SQL = "INSERT " + TABLE + " ("
			+ columns(false) + ") " + "VALUES (" + columns(true) + ")";

	private static final String CREATE_SQL = "create table " + TABLE + " ("
			+ "" + "" + "id number(15,0) primary key,"
			+ "status varchar(1) not null," + "url varchar(2000) not null,"
			+ "host varchar(60) not null," + "port varchar(8) not null,"
			+ "protocol varchar(32),"
			+ "path varchar(255),"
			// parameters
			+ "side varchar(60)," + "timestamp varchar(60),"
			+ "pid varchar(60),"

			+ "application varchar(60)," + "owner varchar(127),"
			+ "interface varchar(255)," + "methods varchar(1000),"

			+ "dubbo varchar(60)" + ");";

	private Map<String, Object> paramMap(URL url) {
		HashMap<String, Object> rs = new HashMap<String, Object>();
		rs.put("status", "1");

		rs.put("url", url.toFullString());
		rs.put("protocol", url.getProtocol());
		rs.put("host", url.getProtocol());
		rs.put("path", url.getProtocol());
		rs.put("port", url.getProtocol());
		rs.put("path", url.getProtocol());

		rs.put("timestamp", url.getParameter("timestamp"));
		rs.put("pid", url.getParameter("pid"));
		rs.put("side", url.getParameter("side"));

		rs.put("application", url.getParameter("application"));
		rs.put("interface", url.getParameter("interface"));
		rs.put("owner", url.getParameter("owner"));
		rs.put("dubbo", url.getParameter("dubbo"));
		rs.put("methods", url.getParameter("methods"));

		return rs;
	}

}
