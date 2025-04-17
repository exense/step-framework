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
package step.core.collections.inmemory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.bson.types.ObjectId;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.accessors.DefaultJacksonMapperProvider;
import step.core.collections.*;
import step.core.collections.Collection;
import step.core.collections.PojoFilters.PojoFilterFactory;
import step.core.collections.AbstractCollection;

public class InMemoryCollection<T> extends AbstractCollection<T> implements Collection<T> {

	private static final Logger logger = LoggerFactory.getLogger(InMemoryCollection.class);
	private final Class<T> entityClass;
	private final Map<ObjectId, T> entities;
	private final ObjectMapper mapper = DefaultJacksonMapperProvider.getObjectMapper();

	private boolean byPassCloning;
	private InMemoryCollectionFactory parentFactory;
	private String name;

	public InMemoryCollection() {
		this(true);
	}

	public InMemoryCollection(String name) {
		this(true, name);
	}

	public InMemoryCollection(boolean byPassCloning) {
		this(byPassCloning, null);
	}

	public InMemoryCollection(boolean byPassCloning, String name) {
		super();
		this.entityClass = null;
		this.entities = new ConcurrentHashMap<>();
		this.byPassCloning = byPassCloning;
		this.name = name;
	}
	
	public InMemoryCollection(InMemoryCollectionFactory parentFactory, String name, Class<T> entityClass, Map<ObjectId, T> entities) {
		super();
		this.parentFactory = parentFactory;
		this.name = name;
		this.entityClass = entityClass;
		this.entities = entities;
	}

	@Override
	public List<String> distinct(String columnName, Filter filter) {
		return filteredStream(filter).map(e -> {
			try {
				Object property = PojoUtils.getProperty(e, columnName);
				return property != null ? property.toString() : null;
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}).distinct().collect(Collectors.toList());
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public long count(Filter filter, Integer limit) {
		Stream<T> stream = find(filter, null, null, null, 0);
		if(limit != null) {
			stream = stream.limit(limit);
		}
		return stream.count();
	}

	@Override
	public long estimatedCount() {
		return entities.size();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Stream<T> find(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime) {
		Stream<T> stream = filteredStream(filter);
		if(order != null && !order.getFieldsSearchOrder().isEmpty()) {
			PojoUtils.SearchOrderComparator<Object> objectSearchOrderComparator = new PojoUtils.SearchOrderComparator<>(order.getFieldsSearchOrder());
			stream = stream.sorted(objectSearchOrderComparator);
		}
		if(skip != null) {
			stream = stream.skip(skip);
		}
		if(limit != null) {
			stream = stream.limit(limit);
		}
		return stream.map(e -> {
			if(entityClass == Document.class && !(e instanceof Document)) {
				return (T) mapper.convertValue(e, Document.class);
			} else if(e instanceof Document && entityClass != Document.class) {
				return mapper.convertValue(e, entityClass);
			} else {
				return (byPassCloning) ? e : clone(e);
			}
		});
	}

	@Override
	public Stream<T> findLazy(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime) {
		return find(filter, order, skip, limit, maxTime);
	}

	@Override
	public Stream<T> findReduced(Filter filter, SearchOrder order, Integer skip, Integer limit, int maxTime, List<String> reduceFields) {
		return find(filter, order, skip, limit, maxTime);
	}

	private Stream<T> filteredStream(Filter filter) {
		PojoFilter<T> pojoFilter = new PojoFilterFactory<T>().buildFilter(filter);
		return entityStream().filter(pojoFilter).sorted(Comparator.comparing(this::getId));
	}

	private Stream<T> entityStream() {
		return entities.values().stream();
	}

	@Override
	public void remove(Filter filter) {
		filteredStream(filter).forEach(f-> entities.remove(getId(f)));
	}

	@Override
	public T save(T entity) {
		if (getId(entity) == null) {
			setId(entity, new ObjectId());
		}
		entities.put(getId(entity), (byPassCloning) ? entity : clone(entity));
		return entity;
	}

	private T clone(T entity) {
		try {
			return (T) mapper.readValue(mapper.writeValueAsString(entity), entity.getClass());
		} catch (JsonProcessingException e) {
			logger.warn("Unable to clone entity before saving into the inMemory collection, returning same instance.");
			if (logger.isDebugEnabled()) {
				logger.debug("Unable to clone entity before saving into the inMemory collection", e);
			}
			return entity;
		}
	}

	@Override
	public void save(Iterable<T> entities) {
		if (entities != null) {
			entities.forEach(this::save);
		}
	}

	@Override
	public void createOrUpdateIndex(String field) {
		createOrUpdateIndex(field, Order.ASC);
	}

	@Override
	public void createOrUpdateIndex(IndexField indexField) {

	}

	@Override
	public void createOrUpdateIndex(String field, Order order) {

	}

	@Override
	public void createOrUpdateCompoundIndex(String... fields) {

	}

	@Override
	public void createOrUpdateCompoundIndex(LinkedHashSet<IndexField> fields) {

	}

	@Override
	public void rename(String newName) {
		//Renaming only make sense when created from a factory
		if (parentFactory != null) {
			parentFactory.renameCollection(name, newName);
		}
		this.name = newName;
	}

	@Override
	public void drop() {
		// This is not 100% accurate, as in principle the collection should disappear as well.
		// But at least, it will be empty.
		remove(Filters.empty());
	}

	@Override
	public Class<T> getEntityClass() {
		return entityClass;
	}

	@Override
	public void dropIndex(String indexName) {

	}
}
