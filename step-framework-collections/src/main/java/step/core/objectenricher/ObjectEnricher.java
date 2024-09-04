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

import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * Instances of this class are responsible for the enrichment of 
 * entities with context parameters. Enrichment refers to the process of
 * adding context parameters to entities that are subject to it
 * (entities implementing {@link EnricheableObject})
 */
public interface ObjectEnricher extends Consumer<EnricheableObject> {
	
	TreeMap<String, String> getAdditionalAttributes();
}
