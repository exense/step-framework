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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.beanutils.PropertyUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.SearchOrder;
import step.core.collections.AbstractCollection;

import java.beans.PropertyDescriptor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PostgreSQLCollection<T> extends AbstractCollection<T> implements Collection<T> {

	private static Pattern p = Pattern.compile("([^.]+)");

	private static final Logger logger = LoggerFactory.getLogger(PostgreSQLCollection.class);

	private final HikariDataSource ds;

	private final String collectionName;
	private final String collectionNameStr;

	private final Class<T> entityClass;

	private final ObjectMapper objectMapper;

	private final String insertOrUpdateQuery;

	public PostgreSQLCollection(HikariDataSource ds, String collectionName, Class<T> entityClass) throws SQLException {
		this.ds = ds;
		this.collectionName = collectionName;
		this.collectionNameStr = "\"" + collectionName + "\"";
		this.entityClass = entityClass;
		objectMapper = PostgreSQLCollectionJacksonMapperProvider.getObjectMapper();
		insertOrUpdateQuery = "INSERT INTO " + collectionNameStr + " (object)\n" +
				"VALUES(?::jsonb) \n" +
				"ON CONFLICT (id) \n" +
				"DO \n" +
				"   UPDATE SET object = ?::jsonb";

		createTableIfRequired();
	}

	private synchronized void createTableIfRequired() throws SQLException {
		try (Connection connection = ds.getConnection()) {
			if (!tableExists(connection)) {
				try (Statement statement = connection.createStatement();) {
					statement.executeUpdate("CREATE TABLE " + collectionNameStr + " (" +
							"id text GENERATED ALWAYS AS  (object ->> 'id') STORED, " +
							"object jsonb NOT NULL,"+
							"PRIMARY KEY (id))");
				}
			}
		}
	}

	private boolean tableExists(Connection connection) throws SQLException {
		DatabaseMetaData meta = connection.getMetaData();
		ResultSet resultSet = meta.getTables(null, null, collectionName, new String[] {"TABLE"});
		return resultSet.next();
	}

	@Override
	public long count(Filter filter, Integer limit) {
		String query = "SELECT count(d.*) FROM (SELECT id FROM " + collectionNameStr +
				" WHERE " + filterToWhereClause(filter) + " LIMIT " + limit + ") d";
		try (Connection connection = ds.getConnection();
			 Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery(query);
			if (resultSet.next()) {
				return resultSet.getInt(1);
			} else {
				throw new RuntimeException("Unable to estimate the count for collection: " + collectionName + ", query: " + query);
			}
		} catch (SQLException e) {
			throw new RuntimeException("Query execution failed: " + query,e);
		}
	}

	@Override
	public long estimatedCount() {
		String query = "SELECT reltuples AS estimate FROM pg_class WHERE relname = '" + collectionName + "'" ;
		try {
			try (Connection connection = ds.getConnection();
				 PreparedStatement preparedStatement = connection.prepareStatement(query);
				 ResultSet resultSet = preparedStatement.executeQuery()) {
				if (resultSet.next()) {
					return resultSet.getInt(1);
				} else {
					throw new RuntimeException("Unable to estimate the count for collection: " + collectionName + " , query: " + query);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Query execution failed: " + query,e);
		}
	}

	@Override
	public Stream<T> find(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime) {
		String query = buildQuery(filter, order, skip, limit);
		try (Connection connection = ds.getConnection();
			 Statement statement = connection.createStatement()){
			if (maxTime > 0) {
				statement.setQueryTimeout(maxTime);
			}
			 try (ResultSet resultSet = statement.executeQuery(query)) {
				 List<String> resultList = new ArrayList<>();
				 while (resultSet.next()) {
					 resultList.add(resultSet.getString(2));
				 }

				 return resultList.stream().map(s-> {
					 try {
						 return objectMapper.readValue(s, entityClass);
					 } catch (JsonProcessingException e) {
						 throw new RuntimeException(e);
					 }
				 });
			 }
		} catch (SQLException e) {
			throw new RuntimeException("Query execution failed: " + query,e);
		}
	}

	private String buildQuery(Filter filter, SearchOrder order, Integer skip, Integer limit) {
		StringBuffer query = new StringBuffer();
		query.append("SELECT * FROM ").append(collectionNameStr).append(" WHERE ").append(filterToWhereClause(filter));
		if (order != null) {
			String sortOrder = (order.getOrder() > 0 ) ? " ASC" : " DESC";
			query.append(" ORDER BY ").append(PostgreSQLFilterFactory.formatField(order.getAttributeName(),Object.class)).append(sortOrder);
		}
		if (skip != null) {
			query.append(" OFFSET ").append(skip);
		}
		if (limit != null) {
			query.append(" LIMIT ").append(limit);
		}
		return query.toString();
	}

	@Override
	public Stream<T> findLazy(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime) {
		String query = buildQuery(filter, order, skip, limit);
		AtomicReference<Connection> connection = new AtomicReference<>();
		AtomicReference<Statement> statement = new AtomicReference<>();
		AtomicReference<ResultSet> resultSet = new AtomicReference<>();
		try {
			connection.set(ds.getConnection());
			statement.set(connection.get().createStatement());
			if (maxTime > 0) {
				statement.get().setQueryTimeout(maxTime);
			}
			resultSet.set(statement.get().executeQuery(query));

			Stream<T> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ResultSetIterator(resultSet.get()),
							Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.ORDERED), false)
					.map(s-> {
						try {
							return objectMapper.readValue(s, entityClass);
						} catch (JsonProcessingException e) {
							throw new RuntimeException(e);
						}
					});
			stream.onClose(() -> safeClose(connection.get(), statement.get(), resultSet.get()));
			return stream;
		} catch (SQLException e) {
			safeClose(connection.get(), statement.get(), resultSet.get());
			throw new RuntimeException("Query execution failed: " + query,e);
		}
	}

	private void safeClose(Connection connection, Statement statement, ResultSet resultSet) {
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				logger.error("Unable to close resultSet" ,e);
			}
		}
		if (statement!= null) {
			try {
				statement.close();
			} catch (SQLException e) {
				logger.error("Unable to close statement" ,e);
			}
		}
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				logger.error("Unable to close connection" ,e);
			}
		}
	}


	@Override
	public Stream<T> findReduced(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime, List<String> reduceFields) {
		//TODO implement proper find reducer? Not sure how to handle this with serializations. Usage only in RTM executeAdvancedQuery
		return find(filter, order, skip,limit,maxTime);
	}

	@Override
	public List<String> distinct(String columnName, Filter filter) {
		Class fieldClass = getFieldClass(columnName);
		StringBuffer query = new StringBuffer();
		query.append("SELECT DISTINCT(").append(PostgreSQLFilterFactory.formatField(columnName,fieldClass)).append(") FROM ").append(collectionNameStr).append(" WHERE ").append(filterToWhereClause(filter));
		try (Connection connection = ds.getConnection();
			 Statement statement = connection.createStatement();
			 ResultSet resultSet = statement.executeQuery(query.toString())){
			List<String> resultList = new ArrayList<>();
			while (resultSet.next()) {
				resultList.add(resultSet.getString(1));
			}
			return resultList;
		} catch (SQLException e) {
			throw new RuntimeException("Query execution failed: " + query,e);
		}
	}

	@Override
	public void remove(Filter filter) {
		executeUpdateQuery("DELETE FROM " + collectionNameStr +
				" WHERE " + filterToWhereClause(filter));
	}

	@Override
	public T save(T entity) {
		if (getId(entity) == null) {
			setId(entity, new ObjectId());
		}
		try (Connection connection = ds.getConnection();
			 PreparedStatement preparedStatement = connection.prepareStatement(insertOrUpdateQuery);) {
			String jsonString = objectMapper.writeValueAsString(entity);
			preparedStatement.setString(1, jsonString);
			preparedStatement.setString(2, jsonString);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
		return entity;
	}

	@Override
	public void save(Iterable<T> entities) {
		try (Connection connection = ds.getConnection();
			 PreparedStatement preparedStatement = connection.prepareStatement(insertOrUpdateQuery);) {
			entities.forEach(entity -> {
				try {
					if (getId(entity) == null) {
						setId(entity, new ObjectId());
					}
					preparedStatement.clearParameters();
					String jsonString = objectMapper.writeValueAsString(entity);
					preparedStatement.setString(1, jsonString);
					preparedStatement.setString(2, jsonString);
					preparedStatement.addBatch();;
				} catch (SQLException ex) {
					throw new RuntimeException(ex);
				} catch (JsonProcessingException ex) {
					throw new RuntimeException(ex);
				}
			});
			preparedStatement.executeBatch();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void createOrUpdateIndex(String field) {
		createOrUpdateIndex(field,1);
	}

	@Override
	public void createOrUpdateIndex(String field, int order) {
		createOrUpdateCompoundIndex(Map.of(field, order));
	}

	@Override
	public void createOrUpdateCompoundIndex(String... fields) {
		Map<String, Integer> fieldsMap = new LinkedHashMap<>();
		Arrays.asList(fields).forEach(s -> fieldsMap.put(s,1));
		createOrUpdateCompoundIndex(fieldsMap);
	}

	@Override
	public void createOrUpdateCompoundIndex(Map<String, Integer> fields) {
		StringBuffer indexId = new StringBuffer().append("idx_").append(collectionName);
		StringBuffer index = new StringBuffer().append("(");
		fields.forEach((k,v) -> {
			String order = (v>0) ? "ASC" : "DESC";
			indexId.append("_").append(k.replaceAll("\\.","_")).append(order);
			//dirty hack to manage RTM (deprecated as of 3.20)
			Class fieldClass = (Document.class.isAssignableFrom(entityClass) && !(k.equals("begin") || k.equals("value"))) ?
					String.class : getFieldClass(k);
			index.append("(").append(PostgreSQLFilterFactory.formatField(k, fieldClass)).append(") ").append(order).append(",");
		});
		//finally replace the last comma by the closing parenthesis
		index.deleteCharAt(index.length()-1).append(")");
		createIndex(indexId.toString(), index.toString());
	}

	private void createIndex(String indexId, String index) {
		String query = "CREATE INDEX IF NOT EXISTS " + indexId + " ON " + collectionNameStr + " " + index;
		if (index.contains("$**")) {
			logger.error("The wildcard not supported for index in postgres, skipping index creation for: " + query);
		} else {
			try (Connection connection = ds.getConnection();
				 Statement statement = connection.createStatement()) {
				statement.executeUpdate(query);
				logger.info("Created index with id: " + indexId);
			} catch (SQLException e) {
				throw new RuntimeException("Unable to create index, query: " + query, e);
			}
		}
	}

	protected Class getFieldClass(String field)  {
		Matcher m = p.matcher(field);
		String previous = null;
		Class currentClass = entityClass;
		while (m.find()) {
			if (previous != null) {
				currentClass = getFieldClass(currentClass,previous);
			}
			previous = m.group(1);
		}
		if (previous == null) {
			throw new RuntimeException("Failed to format jsonb field: " + field);
		}
		return getFieldClass(currentClass,previous);
	}

	protected Class getFieldClass(Class clazz, String _field) {
		String field = _field.equals("_id") ? "id" : _field;
		for (PropertyDescriptor propertyDescriptor: PropertyUtils.getPropertyDescriptors(clazz)) {
			if (propertyDescriptor.getName().equals(field)) {
				Type genericReturnType = propertyDescriptor.getReadMethod().getGenericReturnType();
				if (genericReturnType instanceof ParameterizedType) {
					return (Class) ((ParameterizedType) genericReturnType).getActualTypeArguments()[1];
				} else if (genericReturnType instanceof TypeVariable) {
					return (Class) Arrays.stream(((TypeVariable) genericReturnType).getBounds()).findFirst()
							.orElseThrow(() -> new RuntimeException("Reflection failed for clazz '" + clazz + "' and field '" + field + "'" ));
				} else {
					Class fieldClass = (Class) genericReturnType;
					return (fieldClass.isEnum()) ? String.class: fieldClass;
				}
			}
		}
		if (field.equals("_class")) {
			return String.class;
		} else {
			return clazz;
		}
	}

	@Override
	public void rename(String newName) {
		executeUpdateQuery("ALTER TABLE " + collectionNameStr + " RENAME TO " + newName);
	}

	@Override
	public void drop() {
		executeUpdateQuery("DROP TABLE " + collectionNameStr);

	}

	@Override
	public Class<T> getEntityClass() {
		return entityClass;
	}

	private void executeUpdateQuery(String query) {
		try (Connection connection = ds.getConnection();
			 Statement statement = connection.createStatement()) {
			statement.executeUpdate(query);
		} catch (SQLException e) {
			throw new RuntimeException("Query execution failed: " + query,e);
		}
	}

	private String filterToWhereClause(Filter filter) {
		return new PostgreSQLFilterFactory().buildFilter(filter);
	}
}
