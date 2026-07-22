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

import java.util.Map;

/**
 * The result of a scalar {@link Aggregation}, i.e. of an aggregation reducing its inputs to one single value instead
 * of merging them into a {@link Bucket}.
 * <p>
 * A scalar aggregate is equivalent to a bucket holding one single sample, the scalar itself. The inherited accessors
 * are populated accordingly, so that the consumers reading the generic {@link Bucket} fields get the scalar value
 * instead of an empty bucket.
 */
public class ScalarBucket extends Bucket {

    private long value;

    public ScalarBucket() {
    }

    public ScalarBucket(long value) {
        setValue(value);
    }

    public long getValue() {
        return value;
    }

    /**
     * Sets the scalar value and reports it on the inherited fields as the one single sample of this bucket.
     */
    public void setValue(long value) {
        this.value = value;
        setCount(1);
        setSum(value);
        setMin(value);
        setMax(value);
        // The single sample is held at its exact value, hence a precision of 1
        setPclPrecision(1);
        setDistribution(Map.of(value, 1L));
    }
}
