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

import java.util.List;
import java.util.TreeMap;
import java.util.Objects;
import java.util.stream.Stream;

public class ObjectEnricherComposer {

    public static ObjectEnricher compose(List<ObjectEnricher> list) {
        return new ObjectEnricher() {

            @Override
            public void accept(EnricheableObject o) {
                nonNullList(list).forEach(enricher -> enricher.accept(o));
            }

            @Override
            public TreeMap<String, String> getAdditionalAttributes() {
                TreeMap<String, String> attributes = new TreeMap<>();
                nonNullList(list).forEach(enricher -> attributes.putAll(enricher.getAdditionalAttributes()));
                return attributes;
            }

            private Stream<ObjectEnricher> nonNullList(List<ObjectEnricher> list) {
                return list.stream().filter(Objects::nonNull);
            }
        };
    }

}
