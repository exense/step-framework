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

import java.util.*;
import java.util.stream.Stream;

public class ObjectOverlapperComposer {

    public static ObjectOverlapper compose(List<ObjectOverlapper> list) {
        return new ObjectOverlapper() {

            @Override
            public void onBeforeSave(EnricheableObject obj) {
                nonNullList(list).forEach(overlapper -> overlapper.onBeforeSave(obj));
            }

            @Override
            public <T extends EnricheableObject> List<T> overlapObjects(List<T> objects) {
                if (objects == null) {
                    return null;
                }
                List<T> result = new ArrayList<>(objects);
                Iterator<ObjectOverlapper> iterator = nonNullList(list).iterator();
                while (iterator.hasNext()) {
                    ObjectOverlapper next = iterator.next();
                    result = next.overlapObjects(result);
                }
                return result;
            }


            private Stream<ObjectOverlapper> nonNullList(List<ObjectOverlapper> list) {
                return list.stream().filter(Objects::nonNull);
            }
        };
    }
}
