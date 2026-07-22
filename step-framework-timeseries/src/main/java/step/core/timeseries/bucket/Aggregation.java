/*
 * Copyright (C) 2026, exense GmbH
 *
 * This file is part of Step
 *
 * Step is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Step is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Step.  If not, see <http://www.gnu.org/licenses/>.
 */

package step.core.timeseries.bucket;

import java.util.function.ToLongFunction;

public enum Aggregation {

    MERGE(null),
    AVG(BucketBuilder::getAverage),
    SUM(BucketBuilder::getSum),
    COUNT(BucketBuilder::getCount),
    MIN(BucketBuilder::getMin),
    MAX(BucketBuilder::getMax);

    private final ToLongFunction<BucketBuilder> valueFunction;

    Aggregation(ToLongFunction<BucketBuilder> valueFunction) {
        this.valueFunction = valueFunction;
    }

    /**
     * @return true if this aggregation merges its inputs into a {@link Bucket}, false if it reduces them to a scalar
     */
    public boolean isMerge() {
        return valueFunction == null;
    }

    /**
     * @return true if this aggregation reduces its inputs to a scalar, i.e. to a {@link ScalarBucket}
     */
    public boolean isScalar() {
        return !isMerge();
    }

    /**
     * @return the scalar contributed by the given bucket to the aggregate
     * @throws UnsupportedOperationException if this aggregation is not a scalar one
     */
    public long getValue(BucketBuilder bucket) {
        if (isMerge()) {
            throw new UnsupportedOperationException(name() + " merges its inputs and doesn't reduce them to a scalar");
        }
        return valueFunction.applyAsLong(bucket);
    }
}
