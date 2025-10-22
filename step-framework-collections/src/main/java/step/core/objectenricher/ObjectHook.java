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
package step.core.objectenricher;

import step.core.AbstractContext;

import java.util.Optional;

/**
 * An {@link ObjectHook} is a factory for
 * {@link ObjectFilter} and {@link ObjectEnricher}
 *
 */
public interface ObjectHook {

	ObjectFilter getObjectFilter(AbstractContext context);
	
	ObjectEnricher getObjectEnricher(AbstractContext context);

	/**
	 * Rebuilds an {@link AbstractContext} based on an object that has been
	 * previously enriched with an {@link ObjectEnricher} provided by this class
	 * 
	 * @param context the context to be recreated
	 * @param object the object to base the context reconstruction on
	 * @throws Exception
	 */
	void rebuildContext(AbstractContext context, EnricheableObject object) throws Exception;
	
	/**
	 * Check if the provided object is editable in the provided context
	 *
	 * @param context the context to check access against
	 * @param object the object to check access for
	 * @return ObjectAccessViolation if access is denied, null if access is allowed
	 */
	default Optional<ObjectAccessViolation> isObjectEditableInContext(AbstractContext context, EnricheableObject object){
		return Optional.empty();
	}

	/**
	 * Performs detailed access control check and returns violation details if access is denied.
	 * 
	 * @param context the context to check access against
	 * @param object the object to check access for
	 * @return ObjectAccessViolation if access is denied, null if access is allowed
	 */
	default Optional<ObjectAccessViolation> isObjectReadableInContext(AbstractContext context, EnricheableObject object) {
		return Optional.empty();
	}
	
	/**
	 * Provides a unique identifier for this hook, used in error reporting.
	 * 
	 * @return unique identifier for this hook
	 */
	default String getHookIdentifier() {
		return getClass().getSimpleName();
	}
}
