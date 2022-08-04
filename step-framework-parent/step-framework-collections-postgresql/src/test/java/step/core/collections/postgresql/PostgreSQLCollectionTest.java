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
package step.core.collections.postgresql;

import org.junit.Test;
import step.core.collections.AbstractCollectionTest;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static step.core.collections.postgresql.PostgreSQLFilterFactory.formatField;
import static step.core.collections.postgresql.PostgreSQLFilterFactory.formatFieldForValueAsText;

//Currently no psql server installed on build server and no mocks implemented
public class PostgreSQLCollectionTest { // extends AbstractCollectionTest {

	/*public PostgreSQLCollectionTest() {
		super(new PostgreSQLCollectionFactory(PostgreSQLCollectionTest.getProperties()));
	}*/

	private static Properties getProperties()  {
		Properties properties = new Properties();
		properties.put("jdbcUrl", "jdbc:postgresql://localhost/Test");
		properties.put("user", "postgres");
		properties.put("password", "init");
		return properties;
	}


	@Test
	public void testFieldFormatter() {
		String test;
		test = formatField("id");
		assertEquals("id",test);

		test = formatFieldForValueAsText("id");
		assertEquals("id",test);

		test = formatField("test");
		assertEquals("object->'test'",test);

		test = formatFieldForValueAsText("test");
		assertEquals("object->>'test'",test);

		test = formatField("test.nested");
		assertEquals("object->'test'->'nested'",test);

		test = formatFieldForValueAsText("test.nested");
		assertEquals("object->'test'->>'nested'",test);
	}
}
