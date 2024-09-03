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

import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import step.core.collections.AbstractCollectionTest;
import step.core.collections.Collection;
import step.core.collections.Filters;
import step.core.entities.Bean;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static step.core.collections.postgresql.PostgreSQLFilterFactory.formatField;
import static step.core.collections.postgresql.PostgreSQLFilterFactory.formatFieldForValueAsText;

public class PostgreSQLCollectionTest extends AbstractCollectionTest {

	public PostgreSQLCollectionTest() {
		super(new PostgreSQLCollectionFactory(PostgreSQLCollectionTest.getProperties()));
	}

	private static Properties getProperties()  {
		Properties properties = new Properties();
		properties.put("jdbcUrl", "jdbc:postgresql://central-postgresql.stepcloud-test.ch/Test");
		properties.put("user", "step");
		properties.put("password", "Jua4Nr!46V");
		return properties;
	}

	@Test
	@Ignore
	public void benchmarkInsert() {
		Collection<Bean> beanCollection = collectionFactory.getCollection("Beans", Bean.class);
		beanCollection.remove(Filters.empty());
		int total = 50000;

		for (int loop=1; loop <= 3; loop++) {
			long start = System.currentTimeMillis();
			for (int i = 0; i < total; i++) {
				beanCollection.save(new Bean("property1"));
			}
			long endInsert = System.currentTimeMillis();
			Assert.assertEquals(total*loop, beanCollection.count(Filters.empty(), null));
			System.out.println("insert duration " + (endInsert - start) + " ms, avg: " + ((endInsert - start) / (total * 1.0)) + " ms, count duration: " + (System.currentTimeMillis() - endInsert) + " ms");
		}
	}

	@Test
	@Ignore
	public void benchmarkBulkInsert() {
		Collection<Bean> beanCollection = collectionFactory.getCollection("Beans", Bean.class);
		beanCollection.remove(Filters.empty());
		int total = 50000;
		int bulkSize = 100;

		for (int loop=1; loop <= 3; loop++) {
			long start = System.currentTimeMillis();
			for (int bulks = 0 ; bulks < (total/bulkSize); bulks++) {
				List<Bean> bulk = new ArrayList<>();
				for (int i = 0; i < bulkSize; i++) {
					bulk.add(new Bean("property1"));
				}
				beanCollection.save(bulk);
			}
			long endInsert = System.currentTimeMillis();
			Assert.assertEquals(total*loop, beanCollection.count(Filters.empty(), null));
			System.out.println("insert duration " + (endInsert - start) + " ms, avg: " + ((endInsert - start) / (total * 1.0)) + " ms, count duration: " + (System.currentTimeMillis() - endInsert) + " ms");
		}
	}

	@Test
	public void testGetFieldClass() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
		PostgreSQLCollection<Bean> beanCollection = (PostgreSQLCollection<Bean>) collectionFactory.getCollection("Beans", Bean.class);
		Assert.assertEquals(String.class,beanCollection.getFieldClass("property1"));

		Assert.assertEquals(Long.class, beanCollection.getFieldClass("longProperty"));

		Assert.assertEquals(Boolean.TYPE, beanCollection.getFieldClass("booleanProperty"));

		Assert.assertThrows(UnsupportedOperationException.class, () -> beanCollection.getFieldClass("list"));

		Assert.assertEquals(String.class,beanCollection.getFieldClass("map.property1"));

		Assert.assertEquals(Long.class, beanCollection.getFieldClass("nested.longProperty"));


		Assert.assertEquals(String.class, beanCollection.getFieldClass("simpleBean.stringProperty"));

		Assert.assertEquals(String.class,beanCollection.getFieldClass("map.property1"));

		Assert.assertEquals(JSONObject.class,beanCollection.getFieldClass("jsonOrgObject.key"));

		Assert.assertEquals(String.class,beanCollection.getFieldClass("attributes.property1"));

	}

	@Test
	public void testFieldFormatter() {
		String test;
		test = formatField("id",String.class);
		assertEquals("id",test);

		test = formatField("id", ObjectId.class);
		assertEquals("id",test);

		test = formatField("_id", String.class);
		assertEquals("object->>'id'", test);

		test = formatField("entity._id", String.class);
		assertEquals("object->'entity'->>'id'", test);

		test = formatFieldForValueAsText("id");
		assertEquals("id",test);

		test = formatField("test",Long.class);
		assertEquals("object->'test'",test);

		test = formatField("test",String.class);
		assertEquals("object->>'test'",test);

		test = formatFieldForValueAsText("test");
		assertEquals("object->>'test'",test);

		test = formatField("test.nested",String.class);
		assertEquals("object->'test'->>'nested'",test);

		test = formatField("test.nested",Boolean.class);
		assertEquals("object->'test'->'nested'",test);

		test = formatFieldForValueAsText("test.nested");
		assertEquals("object->'test'->>'nested'",test);
	}
}
