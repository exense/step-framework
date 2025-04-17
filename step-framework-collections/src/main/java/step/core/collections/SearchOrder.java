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
package step.core.collections;

import java.util.List;

public class SearchOrder {

	public final List<FieldSearchOrder> fieldsSearchOrder;

	public static class FieldSearchOrder {
		public final String attributeName;

		public final int order;

		public FieldSearchOrder(String attributeName, int order) {
			this.attributeName = attributeName;
			this.order = order;
		}
	}
	
	public SearchOrder(String attributeName, int order) {
		fieldsSearchOrder = List.of(new FieldSearchOrder(attributeName, order));
	}

	public SearchOrder(List<FieldSearchOrder> fieldsSearchOrder) {
		this.fieldsSearchOrder = fieldsSearchOrder;
	}

	public List<FieldSearchOrder> getFieldsSearchOrder() {
		return fieldsSearchOrder;
	}
}
