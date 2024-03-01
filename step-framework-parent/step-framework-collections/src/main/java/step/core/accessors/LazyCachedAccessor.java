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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.collections.Collection;
import step.core.collections.EntityVersion;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * This {@link Accessor} lazy load entities from the underlying
 * {@link Accessor} in memory. Entities are evicted based on size and time policies
 * The cache is only used when getting entities by IDs. Write operations like remove and save are persisted in the
 * underlying {@link Accessor}
 *
 * @param <T> the type of the entity
 */
public class LazyCachedAccessor<T extends AbstractOrganizableObject> implements Accessor<T> {

	private static final Logger logger = LoggerFactory.getLogger(LazyCachedAccessor.class);
	public static final String UNRESOLVED = "unresolved";
	private final LoadingCache<String, T> cache;

	protected final Accessor<T> underlyingAccessor;

	/**
	 * @param underlyingAccessor the {@link Accessor} from which the entities should
	 *                           be loaded
	 */
	public LazyCachedAccessor(Accessor<T> underlyingAccessor) {
		super();
		this.underlyingAccessor = underlyingAccessor;
		cache = CacheBuilder.newBuilder().concurrencyLevel(4)
				.maximumSize(100)
				.expireAfterAccess(10, TimeUnit.SECONDS)
				.build(new CacheLoader<>() {
					public T load(String id) {
						if (logger.isDebugEnabled()) {
							logger.debug("Id '" + id + "' not found in cache, loading from underlying accessor.");
						}
						return underlyingAccessor.get(id);
					}
				});

	}

	public String tryResolveName(String id) {
		try {
			return this.get(id).getAttribute(AbstractOrganizableObject.NAME);
		} catch (Exception e) {
			return UNRESOLVED;
		}
	}

	@Override
	public Collection<T> getCollectionDriver() {
		return underlyingAccessor.getCollectionDriver();
	}

	@Override
	public T get(ObjectId id) {
        try {
            return cache.get(id.toHexString());
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

	@Override
	public T get(String id) {
        try {
            return cache.get(id);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

	@Override
	public T findByCriteria(Map<String, String> criteria) {
		T byCriteria = underlyingAccessor.findByCriteria(criteria);
		cache.put(byCriteria.getId().toHexString(), byCriteria);
		return byCriteria;
	}

	@Override
	public Stream<T> findByIds(List<String> ids) {
		return ids.stream().map(id -> {
            try {
                return cache.get(id);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
	}

	@Override
	public Stream<T> findManyByCriteria(Map<String, String> criteria) {
		return underlyingAccessor.findManyByCriteria(criteria);
	}
	
	@Override
	public T findByAttributes(Map<String, String> attributes) {
		return underlyingAccessor.findByAttributes(attributes);
	}

	@Override
	public Spliterator<T> findManyByAttributes(Map<String, String> attributes) {
		return underlyingAccessor.findManyByAttributes(attributes);
	}

	@Override
	public T findByAttributes(Map<String, String> attributes, String attributesMapKey) {
		return underlyingAccessor.findByAttributes(attributes, attributesMapKey);
	}

	@Override
	public Spliterator<T> findManyByAttributes(Map<String, String> attributes, String attributesMapKey) {
		return underlyingAccessor.findManyByAttributes(attributes, attributesMapKey);
	}

	@Override
	public Iterator<T> getAll() {
		return underlyingAccessor.getAll();
	}

	@Override
	public void remove(ObjectId id) {
		cache.invalidate(id.toHexString());
		underlyingAccessor.remove(id);
	}

	@Override
	public T save(T entity) {
		T result = underlyingAccessor.save(entity);
		cache.put(result.getId().toHexString(), result);
		return result;
	}

	@Override
	public void save(Iterable<T> entities) {
		entities.forEach(this::save);
	}

	@Override
	public Stream<EntityVersion> getHistory(ObjectId id, Integer skip, Integer limit) {
		return underlyingAccessor.getHistory(id, skip, limit);
	}

	@Override
	public T restoreVersion(ObjectId entityId, ObjectId versionId) {
		T result = underlyingAccessor.restoreVersion(entityId, versionId);
		cache.put(result.getId().toHexString(), result);
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
		return underlyingAccessor.getRange(skip, limit);
	}

	@Override
	public Stream<T> stream() {
		return underlyingAccessor.stream();
	}

	@Override
	public Stream<T> streamLazy() {
		return underlyingAccessor.streamLazy();
	}
}
