package com.belladati.sdk.connector.example.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.sql.DataSource;

import org.postgresql.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.belladati.sdk.connector.ConnectorUtils;
import com.belladati.sdk.connector.DataProviderApi;
import com.belladati.sdk.connector.ProgressBarApi;
import com.belladati.sdk.connector.PropertyValueApi;
import com.belladati.sdk.connector.RowApi;
import com.belladati.sdk.connector.PropertyValueApi.IntegerValue;
import com.belladati.sdk.connector.PropertyValueApi.StringValue;

/**
 * Example implementation of {@link DataProviderApi}. It loads data from PostgreSQL database.
 * @author Lubomir Elko
 */
public class PostgreDataProvider extends DataProviderApi<PostgreRows> {
	private final static Logger log = LoggerFactory.getLogger(PostgreDataProvider.class);

	/** Factory for connections to the PostgreSQL data source that this {@link DataSource} object represents **/
	private DataSource dataSource;

	/** Connection properties used to create {@link DataSource} **/
	private Properties connectionProperties;

	/**
	 * Creates data provider that will get values from PostgreSQL based on the configuration.
	 * @param properties Data provider configuration
	 */
	public PostgreDataProvider(Map<String, PropertyValueApi<?>> properties) {
		super(properties);
	}

	/**
	 * Returns name of this data provider that should be displayed on the user interface.
	 * @return Data provider name
	 */
	public static String getName() {
		return "BellaDati Connector SDK Example - PostgreSQL";
	}

	@Override
	public Map<String, PropertyValueApi<?>> getDefaultProperties() {
		Map<String, PropertyValueApi<?>> defaults = new HashMap<String, PropertyValueApi<?>>();
		defaults.put("host", new StringValue("db.example.com", true));
		defaults.put("port", new IntegerValue(5432, true));
		defaults.put("database", new StringValue("MyDatabaseName", true));
		defaults.put("user", new StringValue(null, false));
		defaults.put("password", new StringValue(null, false, true));
		defaults.put("sqlQuery", new StringValue("SELECT \"column1\", \"column2\" FROM MyTable;", true));
		return defaults;
	}

	@Override
	public PostgreRows providePreviewData(int limit) {
		log.info("Providing preview data: limit=" + limit);
		try {
			Connection connection = createConnection();
			return new PostgreRows(createPreparedStatement(connection, getSqlQuery(), limit));
		} catch (SQLException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public PostgreRows provideImportData(ProgressBarApi progressBar) {
		log.info("Providing import data: progressBar=" + progressBar);
		try {
			Connection connection = createConnection();
			return new PostgreRows(createPreparedStatement(connection, getSqlQuery(), -1), createSizeStatement(connection),
				progressBar);
		} catch (SQLException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public RowApi provideDefaultDataDefinition() {
		log.info("Providing default column names");
		Connection conn = null;
		try {
			PreparedStatement ps = createPreparedStatement(conn = createConnection(), getSqlQuery(), 1);

			ResultSetMetaData metaData = ps.getMetaData();
			final int columnCount = metaData.getColumnCount();
			final String[] headers = new String[columnCount];
			for (int i = 0; i < columnCount; i++) {
				headers[i] = metaData.getColumnLabel(i + 1);
			}
			log.info("Found column names: " + Arrays.toString(headers));

			if (ps != null) {
				ps.close();
			}

			return new RowApi() {

				@Override
				public String[] getValues() {
					return headers;
				}

				@Override
				public String getValue(int columnIndex) {
					return headers[columnIndex];
				}

				@Override
				public int getLength() {
					return columnCount;
				}

				@Override
				public int getIndex() {
					return 0;
				}

			};
		} catch (Exception e) {
			throw new IllegalStateException(e);
		} finally {
			cleanupConnection(conn);
		}
	}

	@Override
	public boolean check() throws Throwable {
		log.info("Checking availability");
		try {
			return checkConnection();
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public List<String> validate() {
		List<String> errors = new ArrayList<String>();

		try {
			checkConnection();
			providePreviewData(1);
		} catch (Throwable e) {
			Throwable error = getCause(e);
			errors.add(error.getClass().getName() + ": " + error.getMessage());
		}

		log.info("Configuration validation ended with " + errors.size() + " errors.");
		return errors;
	}

	private Throwable getCause(Throwable error) {
		if (error.getCause() != null) {
			return getCause(error.getCause());
		} else {
			return error;
		}
	}

	private boolean checkConnection() throws SQLException {
		Connection conn = null;
		try {
			conn = createConnection();
			return true;
		} finally {
			cleanupConnection(conn);
		}
	}

	private Connection createConnection() throws SQLException {
		Connection conn = getOrCreateDataSource().getConnection();
		conn.setAutoCommit(false);
		return conn;
	}

	private void cleanupConnection(Connection conn) {
		try {
			if (conn != null && !conn.isClosed()) {
				conn.commit();
				conn.close();
			}
		} catch (SQLException e) {}
	}

	private DataSource getOrCreateDataSource() {
		Properties currentProperties = getConnectionProperties();
		if (dataSource == null || connectionProperties == null || !connectionProperties.equals(currentProperties)) {
			cleanup();
			connectionProperties = currentProperties;
			dataSource = createDataSource(currentProperties);
		}
		return dataSource;
	}

	private DataSource createDataSource(Properties properties) {
		DriverManagerDataSource ds = new DriverManagerDataSource();
		ds.setDriverClassName(Driver.class.getName());
		ds.setUrl(getConnectionUrl());

		if (properties.get("user") != null) {
			ds.setUsername(properties.getProperty("user"));
		}
		if (properties.get("password") != null) {
			ds.setPassword(properties.getProperty("password"));
		}
		ds.setConnectionProperties(properties);

		log.info("Created DriverManagerDataSource with URL: " + ds.getUrl());
		return ds;
	}

	private Properties getConnectionProperties() {
		Properties p = new Properties();
		for (Entry<String, PropertyValueApi<?>> entry : properties.entrySet()) {
			if (entry.getValue().getValueOrDefault() != null) {
				p.put(entry.getKey(), entry.getValue().getValueOrDefaultAsString());
			}
		}
		return p;
	}

	private String getSqlQuery() {
		return ConnectorUtils.getStringValue(properties, "sqlQuery");
	}

	private String getConnectionUrl() {
		final String host = ConnectorUtils.getStringValue(properties, "host");
		final String database = ConnectorUtils.getStringValue(properties, "database");

		String port = "";
		if (properties.containsKey("port")) {
			port = ":" + ConnectorUtils.getStringValue(properties, "port");
		}

		return "jdbc:postgresql://" + host + port + "/" + database;
	}

	private PreparedStatement createPreparedStatement(Connection connection, String sql, int limit) throws SQLException {
		PreparedStatement ps;
		ps = connection.prepareStatement(sql);
		ps.closeOnCompletion();
		if (limit != -1) {
			ps.setMaxRows(limit);
		}
		return ps;
	}

	private PreparedStatement createSizeStatement(Connection connection) {
		final String innerSql = getSqlQuery().replace(';', ' ').replace('\n', ' ');
		final String countSql = "select count(*) from (" + innerSql + ") as t";

		try {
			PreparedStatement ps = createPreparedStatement(connection, countSql, 1);
			return ps;
		} catch (SQLException e) {
			log.error("Count SQL error: " + countSql, e);
			return null;
		}
	}

	private void cleanup() {
		if (dataSource != null) {
			try {
				cleanupConnection(dataSource.getConnection());
			} catch (Exception e) {}
		}
	}

	@Override
	protected void finalize() throws Throwable {
		cleanup();
		super.finalize();
	}

}
