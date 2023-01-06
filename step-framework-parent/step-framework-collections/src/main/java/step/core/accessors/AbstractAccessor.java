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
package step.core.accessors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.types.ObjectId;

import step.core.collections.Collection;
import step.core.collections.Filter;
import step.core.collections.Filters;
import step.core.collections.SearchOrder;
import step.core.collections.VersionableEntity;
import step.core.collections.filters.Equals;

public class AbstractAccessor<T extends AbstractIdentifiableObject> implements Accessor<T> {

	protected final Collection<T> collectionDriver;

	protected Collection<VersionableEntity> versionedCollectionDriver;

	public AbstractAccessor(Collection<T> collectionDriver) {
		super();
		this.collectionDriver = collectionDriver;
	}

	@Override
	public Collection<T> getCollectionDriver() {
		return collectionDriver;
	}

	@Override
	public T get(ObjectId id) {
		return collectionDriver.find(byId(id), null, null, null, 0).findFirst().orElse(null);
	}

	@Override
	public T get(String id) {
		return get(new ObjectId(id));
	}

	@Override
	public T findByCriteria(Map<String, String> criteria) {
		return findByCriteriaStream(criteria).findFirst().orElse(null);
	}

	@Override
	public Stream<T> findManyByCriteria(Map<String, String> criteria) {
		return findByCriteriaStream(criteria);
	}

	@Override
	public T findByAttributes(Map<String, String> attributes) {
		return findByKeyAttributes("attributes", attributes);
	}

	@Override
	public Spliterator<T> findManyByAttributes(Map<String, String> attributes) {
		return findByAttributesStream("attributes", attributes).spliterator();
	}

	@Override
	public T findByAttributes(Map<String, String> attributes, String attributesMapKey) {
		return findByKeyAttributes(attributesMapKey, attributes);
	}

	@Override
	public Spliterator<T> findManyByAttributes(Map<String, String> attributes, String attributesMapKey) {
		return findByAttributesStream(attributesMapKey, attributes).spliterator();
	}

	private T findByKeyAttributes(String fieldName, Map<String, String> attributes) {
		Stream<T> stream = findByAttributesStream(fieldName, attributes);
		return stream.findFirst().orElse(null);
	}

	private Stream<T> findByAttributesStream(String fieldName, Map<String, String> attributes) {
		attributes = attributes != null ? attributes : new HashMap<>();
		return findByCriteriaStream(attributes.entrySet().stream()
				.collect(Collectors.toMap(e -> fieldName + "." + e.getKey(), e -> e.getValue())));
	}
	
	private Stream<T> findByCriteriaStream(Map<String, String> criteria) {
		Filter filter;
		if(criteria != null && !criteria.isEmpty()) {
			//What is expected if no criteria are provided? Behavior for mongo is no results which seems to be expected for some usages
			filter = Filters.and(criteria.entrySet().stream()
					.map(e -> Filters.equals(e.getKey(), e.getValue())).collect(Collectors.toList()));
		} else {
			filter = Filters.empty();
		}
		return collectionDriver.find(filter, null, null, null, 0);
	}

	@Override
	public Iterator<T> getAll() {
		return stream().iterator();
	}
	
	@Override
	public Stream<T> stream() {
		return collectionDriver.find(Filters.empty(), null, null, null, 0);
	}

	@Override
	public void remove(ObjectId id) {
		collectionDriver.remove(byId(id));
		if (versionedCollectionDriver != null) {
			versionedCollectionDriver.remove(getHistoryFilterById(id));
		}
	}

	private Equals byId(ObjectId id) {
		return Filters.equals("id", id);
	}

	@Override
	public T save(T entity) {
		if (versionedCollectionDriver != null) {
			VersionableEntity version = versionedCollectionDriver.save(new VersionableEntity(entity));
		}
		return collectionDriver.save(entity);
	}

	@Override
	public void save(Iterable<T> entities) {
		if (versionedCollectionDriver != null) {
			List<VersionableEntity> versionedEntities = new ArrayList<>();
			entities.forEach(e->versionedEntities.add(new VersionableEntity<>(e)));
			versionedCollectionDriver.save(versionedEntities);
		}
		collectionDriver.save(entities);
	}

	@Override
	public Stream<VersionableEntity> getHistory(ObjectId id, Integer skip, Integer limit) {
		if (versionedCollectionDriver != null) {
			SearchOrder order = new SearchOrder(AbstractIdentifiableObject.ID, -1);
			return versionedCollectionDriver.find(getHistoryFilterById(id), order, skip, limit, 0);
		} else {
			throw getVersioningNotSupportedException();
		}
	}

	@Override
	public T restoreVersion(ObjectId entityId, ObjectId versionId) {
		if (versionedCollectionDriver != null) {
			VersionableEntity versionableEntity = versionedCollectionDriver.find(byId(versionId),
					null, null, null, 0).findFirst().orElse(null);
			T entity = (T) versionableEntity.getEntity();
			if (entity.getId().equals(entityId)) {
				//save directly with the collection to not create a new version
				return collectionDriver.save(entity);
			} else {
				throw new UnsupportedOperationException("Restore a version from a different entity is not supported, source entity '" +
						entity.getId() + "', target entity '" + entityId + "'.");
			}
		} else {
			throw getVersioningNotSupportedException();
		}
	}

	private Filter getHistoryFilterById(ObjectId id) {
		return Filters.equals("entity._id", id);
	}

	private UnsupportedOperationException getVersioningNotSupportedException() {
		String entityName = (getCollectionDriver() != null && getCollectionDriver().getEntityClass() != null) ?
				getCollectionDriver().getEntityClass().getSimpleName() :
				"unknown";
		return new UnsupportedOperationException("Versioning for the current accessor '" + this.getClass().getName() +
				" and entity '" + entityName + "' is not enabled");
	}

	@Override
	public List<T> getRange(int skip, int limit) {
		return collectionDriver.find(Filters.empty(), null, skip, limit, 0).collect(Collectors.toList());
	}
	
	protected void createOrUpdateIndex(String field) {
		collectionDriver.createOrUpdateIndex(field);
	}
	
	protected void createOrUpdateCompoundIndex(String... fields) {
		collectionDriver.createOrUpdateCompoundIndex(fields);
	}

	@Override
	public void setVersionedCollections(Collection<VersionableEntity> versionedCollection) {
		this.versionedCollectionDriver =  versionedCollection;
	}
}
