/*******************************************************************************
 * Copyright (C) 2020, exense GmbH
 *
 * This file is part of STEP
 *
 * STEP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * STEP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with STEP.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package step.core.collections.postgresql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.CollectionFactory;
import step.core.collections.EntityVersion;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class PostgreSQLCollectionFactory implements CollectionFactory {

	private static final Logger logger = LoggerFactory.getLogger(PostgreSQLCollectionFactory.class);

	private final HikariDataSource ds;

	public PostgreSQLCollectionFactory(Properties properties) {
		super();

		ds = createConnectionPool(properties);
	}

	@Override
	public <T> Collection<T> getCollection(String name, Class<T> entityClass) {
		try {
			return new PostgreSQLCollection<T>(ds, name, entityClass);
		} catch (SQLException e) {
			throw new RuntimeException("Unable to get Jdbc Collection", e);
		}
	}

	@Override
	public Collection<EntityVersion> getVersionedCollection(String name) {
		try {
			return new PostgreSQLCollection(ds, name + CollectionFactory.VERSION_COLLECTION_SUFFIX,
					EntityVersion.class);
		} catch (SQLException e) {
			throw new RuntimeException("Unable to get Jdbc Collection", e);
		}
	}

	@Override
	public void close() throws IOException {
		ds.close();
	}

	static HikariDataSource createConnectionPool(Properties properties) {
		HikariConfig config = new HikariConfig();
		String jdbcUrl = properties.getProperty("jdbcUrl");
		config.setJdbcUrl(jdbcUrl);
		config.setUsername( properties.getProperty("user") );
		config.setPassword( properties.getProperty("password") );
		if (properties.containsKey("minConnections")) {
			config.setMinimumIdle(Integer.parseInt(properties.getProperty("minConnections")));
		}
		config.setMaximumPoolSize(Integer.parseInt(properties.getProperty("maxConnections","50")));
		config.addDataSourceProperty( "cachePrepStmts" , "true" );
		config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
		config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
		// Just to make the default explicit: every connection acquired from the data source will initially have auto-commit on.
		// (this is also the case if the connection is re-used: it will be reset with auto-commit on before being returned)
		config.setAutoCommit( true );
		HikariDataSource hikariDataSource = null;
		try {
			hikariDataSource = new HikariDataSource(config);
			logger.info("Connected to database " + jdbcUrl + ".");
		} catch (HikariPool.PoolInitializationException e ) {
			logger.warn("Unable to connect to the database '"+ jdbcUrl + "', trying to connect to the DB server and create the DB if it does not exist.");
			try {
				createDatabase(properties, jdbcUrl);
				hikariDataSource = new HikariDataSource(config);
			} catch (SQLException ex) {
				logger.error("JDBC connection for url " + jdbcUrl + " failed.",e);
				logger.error("Unable to create the database", ex);
			}

		}
		return hikariDataSource;
	}

	private static void createDatabase(Properties properties, String jdbcUrl) throws SQLException {
		int lastSlash = jdbcUrl.lastIndexOf("/");
		String serverUrl = jdbcUrl.substring(0,lastSlash+1);
		String dbName = jdbcUrl.replace(serverUrl,"").replaceFirst("\\?.*","");
		HikariConfig serverConfig = new HikariConfig();
		serverConfig.setJdbcUrl(serverUrl);
		serverConfig.setUsername( properties.getProperty("user") );
		serverConfig.setPassword( properties.getProperty("password") );
		try (HikariDataSource serverDataSource = new HikariDataSource(serverConfig);
			 Connection connection = serverDataSource.getConnection()) {
			//Connect to DB server

			//Check  the DB is found
			boolean found = false;
			try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
				while (resultSet.next() && !found) {
					// Get the database name, which is at position 1
					found = resultSet.getString(1).equals(dbName);
				}
			}
			if (found) {
				throw new RuntimeException("Database " + dbName + " already exist.");
			} else {
				try (Statement statement = connection.createStatement()){
					statement.executeUpdate("CREATE DATABASE \"" + dbName + "\"");
					logger.info("Database " + dbName + " successfully created.");
				}
			}
		}
	}


}
