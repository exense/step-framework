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

import org.bson.types.ObjectId;

import com.fasterxml.jackson.databind.ObjectMapper;

import step.core.accessors.DefaultJacksonMapperProvider;
import step.core.collections.*;
import step.core.collections.Collection;
import step.core.collections.PojoFilters.PojoFilterFactory;
import step.core.collections.AbstractCollection;

public class InMemoryCollection<T> extends AbstractCollection<T> implements Collection<T> {

	private final Class<T> entityClass;
	private final Map<ObjectId, T> entities;
	private final ObjectMapper mapper = DefaultJacksonMapperProvider.getObjectMapper();
	
	public InMemoryCollection() {
		super();
		entityClass = null;
		entities = new ConcurrentHashMap<>();
	}
	
	public InMemoryCollection(Class<T> entityClass, Map<ObjectId, T> entities) {
		super();
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
		if(order != null) {
			Comparator<T> comparing = (Comparator<T>) PojoUtils.comparator(order.getAttributeName());
			if(order.getOrder()<0) {
				comparing = comparing.reversed();
			}
			stream = stream.sorted(comparing);
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
				return e;
			}
		});
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
		entities.put(getId(entity), entity);
		return entity;
	}

	@Override
	public void save(Iterable<T> entities) {
		if (entities != null) {
			entities.forEach(this::save);
		}
	}

	@Override
	public void createOrUpdateIndex(String field) {
		createOrUpdateIndex(field, 1);
	}

	@Override
	public void createOrUpdateIndex(String field, int order) {

	}

	@Override
	public void createOrUpdateCompoundIndex(String... fields) {
		Map<String,Integer> mapFields = new LinkedHashMap<>();
		Arrays.stream(fields).map(f -> mapFields.put(f,1));
		createOrUpdateCompoundIndex(mapFields);
	}

	@Override
	public void createOrUpdateCompoundIndex(Map<String, Integer> fields) {

	}

	@Override
	public void rename(String newName) {
		// TODO Auto-generated method stub
	}

	@Override
	public void drop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Class<T> getEntityClass() {
		return entityClass;
	}
}
