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
 * Defines how a set of {@link Bucket}s is reduced into a single one. The same set of aggregations is
 * applicable to both axes of the aggregation pipeline, which are independent of each other:
 * <ul>
 *     <li>the <b>time-window aggregation</b>, which reduces the successive buckets of one series into one time bucket</li>
 *     <li>the <b>group-by aggregation</b>, which reduces the aligned buckets of the series of one group into one bucket</li>
 * </ul>
 * <p>
 * An aggregation only defines the <i>scalar value</i> contributed by each input bucket and the <i>weight</i> of that
 * contribution. It never defines how the distribution, the min and the max are merged: those are always merged by
 * union, as this is their only algebraically valid merge. Percentiles obtained from an aggregated bucket are therefore
 * always percentiles over the underlying raw samples, whatever the aggregation in use.
 *
 * @see BucketBuilder#aggregate(Bucket, Aggregation)
 */
public enum Aggregation {

    /**
     * Sample-weighted average: each input bucket contributes its sum, weighted by the number of raw samples it holds.
     * The resulting bucket is a plain merge of its inputs, i.e. it is equivalent to a bucket built out of all the raw
     * samples of the inputs, and it remains mergeable.
     */
    AVG(Bucket::getSum, Aggregation::effectiveContributorCount),

    /**
     * Sum: each input bucket contributes its sum and counts as one single contributor. The average of the resulting
     * bucket is therefore the mean of the sums of its inputs.
     */
    SUM(Bucket::getSum, b -> 1),

    /**
     * Count: each input bucket contributes its number of raw samples and counts as one single contributor. The average
     * of the resulting bucket is therefore the mean of the sample counts of its inputs.
     */
    COUNT(Bucket::getCount, b -> 1);

    private final ToLongFunction<Bucket> valueFunction;
    private final ToLongFunction<Bucket> weightFunction;

    Aggregation(ToLongFunction<Bucket> valueFunction, ToLongFunction<Bucket> weightFunction) {
        this.valueFunction = valueFunction;
        this.weightFunction = weightFunction;
    }

    /**
     * @return the scalar contributed by the given bucket to the aggregate
     */
    public long getValue(Bucket bucket) {
        return valueFunction.applyAsLong(bucket);
    }

    /**
     * @return the number of contributors the given bucket accounts for in the aggregate, i.e. the weight of its value
     */
    public long getWeight(Bucket bucket) {
        return weightFunction.applyAsLong(bucket);
    }

    /**
     * Buckets persisted before the introduction of {@link Bucket#getContributorCount()} carry a contributor count of 0.
     * For those, the number of raw samples is the correct weight, which is also what the contributor count of a freshly
     * ingested bucket amounts to.
     */
    private static long effectiveContributorCount(Bucket bucket) {
        long contributorCount = bucket.getContributorCount();
        return contributorCount > 0 ? contributorCount : bucket.getCount();
    }
}
