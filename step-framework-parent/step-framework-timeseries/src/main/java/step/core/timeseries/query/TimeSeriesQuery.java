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

package step.core.timeseries.query;

import step.core.collections.Filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TimeSeriesQuery {

	// Specified range
	protected Long from;
	protected Long to;

	// Filters
    protected String oqlFilters;
	protected final Map<String, String> filters = new HashMap<>();

    protected Filter filter;

    public TimeSeriesQuery(Long from, Long to, Filter filter) {
        Objects.requireNonNull(filter);

        this.from = from;
        this.to = to;
        this.filter = filter;
    }

//    /**
//	 * Specifies the mandatory list of key-value attributes that the series should match
//	 *
//	 * @param filters the mandatory key-value attributes that the series should match
//	 * @return the builder
//	 */
//	public TimeSeriesQuery filter(Map<String, String> filters) {
//        if (Objects.nonNull(filters)) {
//            this.filters.putAll(filters);
//        }
//		return this;
//	}
//
//    public TimeSeriesQuery filter(String oqlFilters) {
//        if (Objects.nonNull(oqlFilters)) {
//            this.oqlFilters = oqlFilters;
//        }
//        return this;
//    }
//
//
//	protected Map<String, String> getFilters() {
//		return filters;
//	}
//
//	/**
//	 * Specifies a time range for the query
//	 *
//	 * @param from the beginning of the time range in ms (Epoch time)
//	 * @param to   the end of the time range (Epoch time)
//	 * @return the builder
//	 */
//	public TimeSeriesQuery range(long from, long to) {
//		this.from = from;
//		this.to = to;
//		return this;
//	}

	public Long getFrom() {
		return from;
	}

	public Long getTo() {
		return to;
	}

    public Filter getFilter() {
        return filter;
    }

}
