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

/**
 * Defines how a set of {@link Bucket}s is reduced into a single one. The same set of aggregations is applicable to
 * both axes of the aggregation pipeline, which are independent of each other:
 * <ul>
 *     <li>the <b>time-window aggregation</b>, which reduces the successive buckets of one series into one time bucket</li>
 *     <li>the <b>group-by aggregation</b>, which reduces the aligned buckets of the series of one group into one bucket</li>
 * </ul>
 * <p>
 * Two kinds of aggregations are distinguished:
 * <ul>
 *     <li>{@link #MERGE} performs a <i>monoidal merge</i>: the resulting {@link Bucket} is equivalent to a bucket
 *     built out of all the raw samples of its inputs, min, max and distribution included, and it remains mergeable
 *     itself. Being associative and commutative, its result depends neither on the order nor on the grouping of its
 *     inputs.</li>
 *     <li>all the other aggregations are <i>scalar</i>: they reduce their inputs to one single value and therefore
 *     yield a {@link ScalarBucket}.</li>
 * </ul>
 * <p>
 * The two axes compose: each of them reduces the samples the previous one produced, a scalar counting as one single
 * sample. The meaning of an aggregation on the group axis therefore depends on the time aggregation it follows:
 * <ul>
 *     <li><b>MERGE, then MERGE</b>: all the raw samples of the group end up in one bucket. This is the default of
 *     both axes, and the only combination for which the pipeline merges the source buckets directly, without ever
 *     materializing the series they belong to.</li>
 *     <li><b>MERGE, then scalar</b>: the group is reduced to one value computed over all the raw samples of its
 *     series. A group-by {@link #COUNT} is therefore the total number of raw samples, not the number of series.</li>
 *     <li><b>scalar, then MERGE</b>: each series is reduced to one value and the resulting bucket holds one sample
 *     per series. Its percentiles are percentiles over the per-series values, not over the raw samples.</li>
 *     <li><b>scalar, then scalar</b>: each series is reduced to one value and the group is reduced to one value over
 *     those. A group-by {@link #MIN} is therefore the lowest per-series value, not the lowest raw sample.</li>
 * </ul>
 *
 * @see BucketBuilder#aggregate(BucketBuilder)
 */
public enum Aggregation {

    /**
     * Merges the samples of its inputs instead of reducing them to a scalar. The resulting bucket keeps the sum, the
     * count, the min, the max and the distribution of all its inputs, and remains mergeable.
     */
    MERGE(null),

    /**
     * Average of the samples of the aggregate, i.e. their sum divided by their number.
     */
    AVG(BucketBuilder::getAverage),

    /**
     * Sum of the samples of the aggregate.
     */
    SUM(BucketBuilder::getSum),

    /**
     * Number of samples of the aggregate. Beware that a series reduced by a scalar time aggregation contributes one
     * single sample, whereas a merged series contributes all its raw samples.
     */
    COUNT(BucketBuilder::getCount),

    /**
     * Lowest sample of the aggregate.
     */
    MIN(BucketBuilder::getMin),

    /**
     * Highest sample of the aggregate.
     */
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
     * Reduces the samples accumulated by the given builder to the scalar defined by this aggregation.
     *
     * @param builder the builder holding the samples to reduce
     * @return the scalar the given builder amounts to
     * @throws UnsupportedOperationException if this aggregation is not a scalar one
     */
    public long getValue(BucketBuilder builder) {
        if (isMerge()) {
            throw new UnsupportedOperationException(name() + " merges its inputs and doesn't reduce them to a scalar");
        }
        return valueFunction.applyAsLong(builder);
    }
}
