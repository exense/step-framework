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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.types.ObjectId;
import step.core.collections.Collection;
import step.core.collections.EntityVersion;

public class LayeredAccessor<T extends AbstractIdentifiableObject> implements Accessor<T> {

	private final List<Accessor<T>> accessors = new ArrayList<>();
	
	public LayeredAccessor() {
		super();
	}
	
	public LayeredAccessor(List<? extends Accessor<T>> accessors) {
		super();
		this.accessors.addAll(accessors);
	}

	public void addAccessor(Accessor<T> accessor) {
		accessors.add(accessor);
	}

	public void pushAccessor(Accessor<T> accessor) {
		accessors.add(0, accessor);
	}

	@Override
	public Collection<T> getCollectionDriver() {
		return layeredLookup(a->getCollectionDriver());
	}

	@Override
	public T get(ObjectId id) {
		return layeredLookup(a->a.get(id));
	}

	protected <V> V layeredLookup(Function<Accessor<T>, V> f) {
		for (Accessor<T> Accessor : accessors) {
			V result = f.apply(Accessor);
			if(result != null) {
				return result;
			}
		}
		return null;
	}

	@Override
	public T get(String id) {
		return get(new ObjectId(id));
	}

	@Override
	public T findByCriteria(Map<String, String> criteria) {
		return layeredLookup(a -> a.findByCriteria(criteria));
	}

	@Override
	public Stream<T> findManyByCriteria(Map<String, String> criteria) {
		return layeredStreamMerge(a -> a.findManyByCriteria(criteria));
	}

	@Override
	public T findByAttributes(Map<String, String> attributes) {
		return layeredLookup(a -> a.findByAttributes(attributes));
	}

	@Override
	public Spliterator<T> findManyByAttributes(Map<String, String> attributes) {
		return layeredMerge(a -> a.findManyByAttributes(attributes));
	}

	protected <V> Stream<V> layeredStreamMerge(Function<Accessor<T>, Stream<V>> f) {
		return accessors.stream().map(a -> f.apply(a)).flatMap(i -> i);
	}
	
	protected <V> Spliterator<V> layeredMerge(Function<Accessor<T>, Spliterator<V>> f) {
		List<V> result = new ArrayList<>();
		accessors.forEach(a->{
			f.apply(a).forEachRemaining(result::add);	
		});
		return result.spliterator();
	}
	
	@Override
	public Iterator<T> getAll() {
		return layeredStreamMerge(a -> a.stream()).iterator();
	}
	
	@Override
	public T findByAttributes(Map<String, String> attributes, String attributesMapKey) {
		return layeredLookup(a->a.findByAttributes(attributes, attributesMapKey));
	}

	@Override
	public Spliterator<T> findManyByAttributes(Map<String, String> attributes, String attributesMapKey) {
		return layeredMerge(a->a.findManyByAttributes(attributes, attributesMapKey));
	}

	@Override
	public List<T> getRange(int skip, int limit) {
		return stream().skip(skip).limit(limit).collect(Collectors.toList());
	}

	@Override
	public void remove(ObjectId id) {
		for (Accessor<T> Accessor : accessors) {
			T e = Accessor.get(id);
			if(e!= null) {
				Accessor.remove(id);
			}
		}
	}

	@Override
	public T save(T entity) {
		return getAccessorForPersistence().save(entity);
	}

	@Override
	public void save(Iterable<T> entities) {
		getAccessorForPersistence().save(entities);
	}

	@Override
	public Stream<EntityVersion> getHistory(ObjectId id, Integer skip, Integer limit) {
		return layeredLookup(a->a.isVersioningEnabled() ? a.getHistory(id, skip, limit): null);
	}

	@Override
	public T restoreVersion(ObjectId entityId, ObjectId versionId) {
		return layeredLookup(a->a.isVersioningEnabled() ? a.restoreVersion(entityId, versionId) : null);
	}

	@Override
	public boolean isVersioningEnabled() {
		return false;
	}

	@Override
	public void enableVersioning(Collection<EntityVersion> versionedCollection, Long newVersionThresholdMs) {
		throw new UnsupportedOperationException("The versioned collections cannot be set on the layered accessor, but on individual accessors only");
	}

	protected Accessor<T> getAccessorForPersistence() {
		return accessors.get(0);
	}

	@Override
	public Stream<T> stream() {
		return layeredStreamMerge(a -> a.stream());
	}
}
