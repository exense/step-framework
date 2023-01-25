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
import step.core.collections.VersionableEntity;

public interface Accessor<T extends AbstractIdentifiableObject> {

	Collection<T> getCollectionDriver();

	/**
	 * Get an object by id
	 * 
	 * @param id the UID of the object
	 * @return the object
	 */
	T get(ObjectId id);
	
	/**
	 * Get an object by id
	 * 
	 * @param id the UID of the object
	 * @return the object
	 */
	T get(String id);

	/**
	 * Find an object by criteria. If multiple objects match these criteria, the first one will be returned
	 * 
	 * @param criteria the map of mandatory criteria of the object to be found
	 * @return the object
	 */
	T findByCriteria(Map<String, String> criteria);
	
	/**
	 * Find objects by criteria.
	 * 
	 * @param criteria the map of mandatory criteria of the object to be found
	 * @return an {@link Iterator} for the objects found
	 */
	Stream<T> findManyByCriteria(Map<String, String> criteria);
	
	/**
	 * Find an object by default attributes. If multiple objects match these attributes, the first one will be returned
	 * 
	 * @param attributes the map of mandatory attributes of the object to be found
	 * @return the object
	 */
	T findByAttributes(Map<String, String> attributes);
	
	/**
	 * Find objects by attributes.
	 * 
	 * @param attributes the map of mandatory attributes of the object to be found
	 * @return an {@link Iterator} for the objects found
	 */
	Spliterator<T> findManyByAttributes(Map<String, String> attributes);

	Iterator<T> getAll();
	
	/**
	 * @return a {@link Stream} with all objects
	 */
	Stream<T> stream();

	/**
	 * Find an object by attributes. If multiple objects match these attributes, the first one will be returned
	 * 
	 * @param attributes the map of mandatory attributes of the object to be found
	 * @param attributesMapKey the string representing the name (or "key") of the attribute map
	 * @return the object
	 */
	T findByAttributes(Map<String, String> attributes, String attributesMapKey);

	/**
	 * Find objects by attributes.
	 * 
	 * @param attributes the map of mandatory attributes of the object to be found
	 * @param attributesMapKey the string representing the name (or "key") of the attribute map
	 * @return an {@link Iterator} for the objects found
	 */
	Spliterator<T> findManyByAttributes(Map<String, String> attributes, String attributesMapKey);
	
	/**
	 * Get the range of objects specified by the skip/limit parameters browsing the collection 
	 * sorted by ID in the descending order  
	 * 
	 * @param skip the start index (inclusive) of the range
	 * @param limit the size of the range
	 * @return a {@link List} containing the objects of the specified range
	 */
	List<T> getRange(int skip, int limit);
	
	/**
	 * Remove an entity.
	 * 
	 * @param id id the entity
	 */
	void remove(ObjectId id);

	/**
	 * Save an entity. If an entity with the same id exists, it will be updated otherwise inserted. 
	 * 
	 * @param entity the entity instance to be saved
	 * @return the saved entity
	 */
	T save(T entity);

	/**
	 * Save a list of entities. 
	 * 
	 * @param entities the list of entities to be saved
	 */
	void save(Iterable<T> entities);

	/**
	 * Get all object's versions by id
	 *
	 * @param id the UID of the object
	 * @return the list of versioned objects
	 */
	Stream<VersionableEntity> getHistory(ObjectId id, Integer skip, Integer limit);

	/**
	 * Restore a version
	 *
	 * @param entityId the UID of the object to be restored
	 * @param entityId the UID of the object to be restored
	 * @return the restored object
	 */
	T restoreVersion(ObjectId entityId, ObjectId versionId);

	void enableVersioning(Collection<VersionableEntity> versionedCollection, Long newVersionThresholdMs);
}
