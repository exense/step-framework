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
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Instances of this class are responsible for the enrichment of
 * entities with context parameters. Enrichment refers to the process of
 * adding context parameters to entities that are subject to it
 * (entities implementing {@link EnricheableObject})
 */
public interface ObjectEnricher extends Consumer<EnricheableObject> {

    /**
     * Returns the context attributes (key/value pairs) contributed by this enricher.
     * <p>
     * The key order is guaranteed to be consistent across all invocations (ensured by
     * {@link TreeMap}), and matches the order of {@link #getAdditionalAttributeKeys()}.
     * Consistent ordering is essential for callers that register a structure (such as a
     * label schema) based on the keys on a first call, and then populate it by position
     * on subsequent calls — without this guarantee, callers would have to re-sort or
     * perform individual key lookups on every invocation. The contract is enforced at
     * the interface level so that all implementations remain consistent.
     *
     * @return the context attributes in natural key order
     */
    default TreeMap<String, String> getAdditionalAttributes() {
        return new TreeMap<>();
    }

    /**
     * Returns the keys of the context attributes contributed by this enricher.
     * <p>
     * Keys are in natural order (guaranteed by {@link TreeSet}), consistent across all
     * invocations and matching the key order of {@link #getAdditionalAttributes()}.
     *
     * @return the context attribute keys in natural order
     */
    default TreeSet<String> getAdditionalAttributeKeys() {
        return new TreeSet<>();
    }
}
