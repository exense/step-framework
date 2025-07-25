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
import com.zaxxer.hikari.pool.HikariProxyStatement;
import org.apache.commons.beanutils.PropertyUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.*;
import step.core.collections.AbstractCollection;
import step.core.collections.Collection;

import java.beans.PropertyDescriptor;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@SuppressWarnings({"SqlSourceToSinkFlow", "SqlNoDataSourceInspection"})
public class PostgreSQLCollection<T> extends AbstractCollection<T> implements Collection<T> {

	private static Pattern p = Pattern.compile("([^.]+)");

	private static final Logger logger = LoggerFactory.getLogger(PostgreSQLCollection.class);

	private final HikariDataSource ds;

	private String collectionName;
	private String collectionNameStr;

	private final Class<T> entityClass;

	private final ObjectMapper objectMapper;

	private final String insertOrUpdateQuery;

	public PostgreSQLCollection(HikariDataSource ds, String collectionName, Class<T> entityClass) throws SQLException {
		this.ds = ds;
		this.collectionName = collectionName;
		this.collectionNameStr = getCollectionNameStr(collectionName);
		this.entityClass = entityClass;
		objectMapper = PostgreSQLCollectionJacksonMapperProvider.getObjectMapper();
		insertOrUpdateQuery = "INSERT INTO " + collectionNameStr + " (object)\n" +
				"VALUES(?::jsonb) \n" +
				"ON CONFLICT (id) \n" +
				"DO \n" +
				"   UPDATE SET object = ?::jsonb";

		createTableIfRequired();
	}

	private static String getCollectionNameStr(String collectionName) {
		return "\"" + collectionName + "\"";
	}

	private synchronized void createTableIfRequired() throws SQLException {
		try (Connection connection = ds.getConnection()) {
			if (!tableExists(connection)) {
				try (Statement statement = connection.createStatement()) {
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
	public String getName() {
		return collectionName;
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
		String query = "SELECT reltuples AS estimate FROM pg_class WHERE relname = ?";
		try {
			try (Connection connection = ds.getConnection();
				 PreparedStatement preparedStatement = connection.prepareStatement(query)) {
				preparedStatement.setString(1, collectionName);
				try (ResultSet resultSet = preparedStatement.executeQuery()) {
					if (resultSet.next()) {
						return resultSet.getInt(1);
					} else {
						throw new RuntimeException("Unable to estimate the count for collection: " + collectionName + " , query: " + query);
					}
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Query execution failed: " + query, e);
		}
	}

	@Override
	public Stream<T> find(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime) {
		String query = buildQuery(filter, order, skip, limit);
		List<String> resultList = new ArrayList<>();
		try (StreamingQuery sq = new StreamingQuery(ds, query, maxTime)) {
			while (sq.resultSet.next()) {
				resultList.add(sq.resultSet.getString(2));
			}
		} catch (SQLException e) {
			throw new RuntimeException("Query execution failed: " + query, e);
		}
		return resultList.stream().map(s -> {
			try {
				return objectMapper.readValue(s, entityClass);
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		});
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
		if (logger.isDebugEnabled()) {
			logger.debug("Executing query: " + query);
		}
		return query.toString();
	}

	@Override
	public Stream<T> findLazy(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime) {
		String query = buildQuery(filter, order, skip, limit);
		AtomicReference<StreamingQuery> queryRef = new AtomicReference<>();
		try {
			queryRef.set(new StreamingQuery(ds, query));
			return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ResultSetIterator(queryRef.get().resultSet),
							Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.ORDERED), false)
					.map(s -> {
						try {
							return objectMapper.readValue(s, entityClass);
						} catch (JsonProcessingException e) {
							throw new RuntimeException(e);
						}
					}).onClose(() -> safeClose(queryRef.get()));
		} catch (SQLException e) {
			safeClose(queryRef.get());
			throw new RuntimeException("Query execution failed: " + query, e);
		}
	}

	private void safeClose(StreamingQuery streamingQuery) {
		try {
			if (streamingQuery != null) {
				streamingQuery.close();
			}
		} catch (Exception e) {
			logger.error("Exception while closing {}", streamingQuery.getClass().getSimpleName(), e);
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
		try (StreamingQuery sq = new StreamingQuery(ds, query.toString())){
			List<String> resultList = new ArrayList<>();
			while (sq.resultSet.next()) {
				resultList.add(sq.resultSet.getString(1));
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
		createOrUpdateIndex(field, Order.ASC);
	}

	@Override
	public void createOrUpdateIndex(IndexField indexField) {
		createOrUpdateCompoundIndex(new LinkedHashSet<>(List.of(indexField)));
	}

	@Override
	public void createOrUpdateIndex(String field, Order order) {
		createOrUpdateCompoundIndex(new LinkedHashSet<>(List.of(new IndexField(field, order, null))));
	}

	@Override
	public void createOrUpdateCompoundIndex(String... fields) {
		LinkedHashSet<IndexField> setIndexField = Arrays.stream(fields).map(f -> new IndexField(f, Order.ASC, null)).collect(Collectors.toCollection(LinkedHashSet::new));
		createOrUpdateCompoundIndex(setIndexField);
	}

	@Override
	public void createOrUpdateCompoundIndex(LinkedHashSet<IndexField> fields) {
		StringBuffer indexId = new StringBuffer().append("idx_").append(collectionName);
		StringBuffer index = new StringBuffer().append("(");
		fields.forEach(i -> {
			String fieldName = i.fieldName;
			String order = i.order.name();
			indexId.append("_").append(fieldName.replaceAll("\\.","_")).append(order);
			Class<?> fieldClass = Objects.requireNonNullElseGet(i.fieldClass, () -> getFieldClass(fieldName));
			if (fieldClass.getClass().equals(Object.class)) {
				throw new UnsupportedOperationException("Creation of index on fields with resolved type 'Object' is not supported, use the index creation method specifying the type explicitly");
			}
			index.append("(").append(PostgreSQLFilterFactory.formatField(fieldName, fieldClass)).append(") ").append(order).append(",");
		});
		//finally replace the last comma by the closing parenthesis
		index.deleteCharAt(index.length()-1).append(")");
		createIndex(indexId.toString(), index.toString());
	}

	private void createIndex(String indexId, String index) {
		String query = "CREATE INDEX IF NOT EXISTS " + indexId + " ON " + collectionNameStr + " " + index;
		if (index.contains("$**")) {
			logger.error("The wildcard is not supported for index in postgres, skipping index creation for: " + query);
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

	/**
	 * Recursively find the field passed in argument in the entityClass of this Collection and return its class
	 * This method is used to determine whether this field should be treated as text or object (required when building indexes,
	 * for distinct queries and for order by clause)
	 * Note that it only supports a subset of class fields, only map parametrized type are supported
	 * @param field the name of the field to be found
	 * @return the Class of the field
	 */
	protected Class<?> getFieldClass(String field)  {
		Matcher m = p.matcher(field);
		String previous = null;
		Class<?> currentClass = entityClass;
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

	protected Class<?> getFieldClass(Class<?> clazz, String _field) {
		String fieldName = _field.equals("_id") ? "id" : _field;
		if (fieldName.equals("_class")) {
			return String.class;
		}
		//Try with getters
		for (PropertyDescriptor propertyDescriptor: PropertyUtils.getPropertyDescriptors(clazz)) {
			if (propertyDescriptor.getName().equals(fieldName)) {
				Type genericReturnType = propertyDescriptor.getReadMethod().getGenericReturnType();
				return getFieldClassForActualField(clazz, fieldName, genericReturnType);
			}
		}
		//try with public fields
		for (Field field : clazz.getFields()) {
			//getFields only return public fields, but the check is cheap, so keeping it for safety and clarity
			if (Modifier.isPublic(field.getModifiers()) && fieldName.equals(field.getName())) {
				Type genericType = field.getGenericType();
				return getFieldClassForActualField(clazz, fieldName, genericType);
			}
		}
		//current property was not found, this happens for nested fields in Maps
		return clazz;
    }

	protected Class<?> getFieldClassForActualField(Class<?> clazz, String fieldName, Type genericType) {
		if (genericType instanceof ParameterizedType) {
			ParameterizedType pt = (ParameterizedType) genericType;
			Class<?> rawType = (Class<?>) pt.getRawType();

			if (Map.class.isAssignableFrom(rawType) && pt.getActualTypeArguments().length == 2) {
				// Get the value type of the Map
				Type valueType = pt.getActualTypeArguments()[1];
				if (valueType instanceof Class<?>) {
					return (Class<?>) valueType;
				} else if (valueType instanceof ParameterizedType) {
					return (Class<?>) ((ParameterizedType) valueType).getRawType();
				} else {
					throw new UnsupportedOperationException("Unable to retrieve type of ParameterizedType field '" + fieldName + "' for class '" + clazz.getSimpleName());
				}
			} else {
				throw new UnsupportedOperationException("Unable to retrieve type of ParameterizedType field '" +
						fieldName + "' for class '" + clazz.getSimpleName() + "'");
			}
		} else if (genericType instanceof TypeVariable) {
			// Handle generic type variables (e.g., T)
			Type[] bounds = ((TypeVariable<?>) genericType).getBounds();
			if (bounds.length == 0) {
				throw  new RuntimeException("Reflection failed for clazz '" + clazz + "' and field '" + fieldName + "'" );
			}
			return (Class<?>) bounds[0];
		} else if (genericType instanceof Class<?>) {
			Class<?> fieldClass = (Class<?>) genericType;
			return (fieldClass.isEnum()) ? String.class : fieldClass;
		} else {
			throw new UnsupportedOperationException("Unsupported generic type: " + genericType.getTypeName());
		}
	}

	@Override
	public void rename(String newName) {
		executeUpdateQuery("ALTER TABLE " + collectionNameStr + " RENAME TO " + newName);
		collectionName = newName;
		collectionNameStr = getCollectionNameStr(collectionName);
	}

	@Override
	public void drop() {
		executeUpdateQuery("DROP TABLE " + collectionNameStr);

	}

	@Override
	public Class<T> getEntityClass() {
		return entityClass;
	}

	@Override
	public void dropIndex(String indexName) {
		executeUpdateQuery("DROP INDEX IF EXISTS " + indexName);
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
