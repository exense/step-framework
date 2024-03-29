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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Stream;

import org.bson.types.ObjectId;
import step.core.collections.Collection;
import step.core.collections.EntityVersion;

/**
 * This {@link Accessor} loads all the entities of the provided underlying
 * {@link Accessor} at initialization and keeps them in memory for further
 * accesses. Write operations like remove and save are persisted in the
 * underlying {@link Accessor}
 *
 * @param <T> the type of the entity
 */
public class CachedAccessor<T extends AbstractIdentifiableObject> implements Accessor<T> {

	private final Accessor<T> cache;

	private final Accessor<T> underlyingAccessor;

	/**
	 * @param underlyingAccessor the {@link Accessor} from which the entities should
	 *                           be loaded
	 */
	public CachedAccessor(Accessor<T> underlyingAccessor) {
		this(underlyingAccessor, true);
	}

	public CachedAccessor(Accessor<T> underlyingAccessor, boolean byPassCloning) {
		super();
		cache = new InMemoryAccessor<T>(byPassCloning);
		this.underlyingAccessor = underlyingAccessor;
		reloadCache();
	}

	/**
	 * Reloads all the entities from the underlying {@link Accessor}
	 */
	public void reloadCache() {
		// Load cache
		underlyingAccessor.getAll().forEachRemaining(e -> cache.save(e));
	}

	@Override
	public Collection<T> getCollectionDriver() {
		return underlyingAccessor.getCollectionDriver();
	}

	@Override
	public T get(ObjectId id) {
		return cache.get(id);
	}

	@Override
	public T get(String id) {
		return cache.get(id);
	}

	@Override
	public T findByCriteria(Map<String, String> criteria) {
		return cache.findByCriteria(criteria);
	}

	@Override
	public Stream<T> findByIds(List<String> ids) {
		return cache.findByIds(ids);
	}

	@Override
	public Stream<T> findManyByCriteria(Map<String, String> criteria) {
		return cache.findManyByCriteria(criteria);
	}
	
	@Override
	public T findByAttributes(Map<String, String> attributes) {
		return cache.findByAttributes(attributes);
	}

	@Override
	public Spliterator<T> findManyByAttributes(Map<String, String> attributes) {
		return cache.findManyByAttributes(attributes);
	}

	@Override
	public T findByAttributes(Map<String, String> attributes, String attributesMapKey) {
		return cache.findByAttributes(attributes, attributesMapKey);
	}

	@Override
	public Spliterator<T> findManyByAttributes(Map<String, String> attributes, String attributesMapKey) {
		return cache.findManyByAttributes(attributes, attributesMapKey);
	}

	@Override
	public Iterator<T> getAll() {
		return cache.getAll();
	}

	@Override
	public void remove(ObjectId id) {
		cache.remove(id);
		underlyingAccessor.remove(id);
	}

	@Override
	public T save(T entity) {
		T result = underlyingAccessor.save(entity);
		cache.save(result);
		return result;
	}

	@Override
	public void save(Iterable<T> entities) {
		entities.forEach(e -> save(e));
	}

	@Override
	public Stream<EntityVersion> getHistory(ObjectId id, Integer skip, Integer limit) {
		return underlyingAccessor.getHistory(id, skip, limit);
	}

	@Override
	public T restoreVersion(ObjectId entityId, ObjectId versionId) {
		T result = underlyingAccessor.restoreVersion(entityId, versionId);
		cache.save(result);
		return result;
	}

	@Override
	public boolean isVersioningEnabled() {
		return underlyingAccessor.isVersioningEnabled();
	}

	@Override
	public void enableVersioning(Collection<EntityVersion> versionedCollection, Long newVersionThresholdMs) {
		throw new UnsupportedOperationException("The versioned collections cannot be set on the cached accessor, but on its underlying accessors only");
	}

	@Override
	public List<T> getRange(int skip, int limit) {
		return cache.getRange(skip, limit);
	}

	@Override
	public Stream<T> stream() {
		return cache.stream();
	}

	@Override
	public Stream<T> streamLazy() {
		return cache.streamLazy();
	}
}
