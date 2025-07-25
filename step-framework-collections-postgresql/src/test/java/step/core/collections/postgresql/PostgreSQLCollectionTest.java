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

import com.zaxxer.hikari.HikariDataSource;
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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

		Assert.assertEquals(String.class,beanCollection.getFieldClass("publicFinalField"));
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

	private static class MyReport {
		protected String executionID;
	}

	// SED-4114 - Before the fix, this would have caused an OOM.
	@Ignore // Run this manually. Plus (this is important!!!) run it with -Xmx50M.
	@SuppressWarnings("SqlNoDataSourceInspection")
	@Test
	public void testLargeDataSet() throws Exception {
		// This test might take multiple minutes.
		HikariDataSource ds = PostgreSQLCollectionFactory.createConnectionPool(getProperties());
		Connection connection = ds.getConnection();
		assertTrue(connection.getAutoCommit());

		String tablename = "oomtest";
		Function<String, Integer> execUpdate = (sql) -> {
			try (Statement stmt = connection.createStatement()) {
				System.out.println(sql);
				//noinspection SqlSourceToSinkFlow // IntelliJ, shut up!
				return stmt.executeUpdate(sql);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		};

		Callable<Long> count = () -> {
			try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery("select count(*) from " + tablename)) {
				return rs.next() ? rs.getLong(1) : 0;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		};

		try {
			count.call();
		} catch (Exception e) {
			// Assuming table doesn't exist
			assertEquals(0, execUpdate.apply("DROP TABLE IF EXISTS " + tablename).intValue());
			assertEquals(0, execUpdate.apply("CREATE TABLE " + tablename + "(id text, object jsonb)").intValue());
			assertEquals(0, count.call().longValue());

			// populate data
			// 2. JSON payload (escaped and quoted)
			String json = "{\"id\": \"68820f2ef74c111105c56454\", \"name\": \"NOP\", \"path\": \"68820f2ef74c111105c56454\", \"error\": null, \"_class\": \"step.artefacts.reports.TestCaseReportNode\", \"orphan\": false, \"status\": \"PASSED\", \"duration\": 597, \"parentID\": \"68820f2ef74c111105c563f5\", \"artefactID\": \"6881fd2f40bd6c67ea0d970b\", \"attachments\": [], \"executionID\": \"68820f2ef74c111105c563f5\", \"artefactHash\": \"F31CCC37FEFC08542360607EA9795DCB\", \"customFields\": null, \"parentSource\": \"MAIN\", \"executionTime\": 1753354030955, \"customAttributes\": {\"TestCase\": \"6881fd2f40bd6c67ea0d970b\"}, \"resolvedArtefact\": {\"id\": \"6881fd2f40bd6c67ea0d970b\", \"after\": null, \"_class\": \"TestCase\", \"before\": null, \"children\": null, \"skipNode\": {\"value\": false, \"dynamic\": false, \"expression\": null, \"expressionType\": null}, \"attributes\": {\"name\": \"NOP\"}, \"attachments\": null, \"description\": null, \"dynamicName\": {\"value\": \"\", \"dynamic\": false, \"expression\": \"\", \"expressionType\": null}, \"customFields\": null, \"workArtefact\": false, \"instrumentNode\": {\"value\": false, \"dynamic\": false, \"expression\": null, \"expressionType\": null}, \"useDynamicName\": false, \"customAttributes\": null, \"continueParentNodeExecutionOnError\": {\"value\": false, \"dynamic\": false, \"expression\": null, \"expressionType\": null}}, \"contributingError\": null}";

			// 3. Build the SQL query string
			String insertSql =
					"INSERT INTO " + tablename + " (id, object) " +
							"SELECT '68820f2ef74c111105c56454', '" + json.replace("'", "''") + "'::jsonb " +
							"FROM generate_series(1, 1000000);";

			// 4. Execute bulk insert, this should take about a minute
			assertEquals(1000000, execUpdate.apply(insertSql).intValue());
		}
		assertEquals(1000000, count.call().longValue());

		// OK, now onto the actual test
		AtomicInteger all = new AtomicInteger(0);
		Collection<MyReport> coll = collectionFactory.getCollection(tablename, MyReport.class);
		long d = System.currentTimeMillis();
		try (Stream<MyReport> s = coll.findLazy(Filters.empty(), null, null, null, 0)) {
			s.forEach(o -> all.incrementAndGet());
		}
		d = System.currentTimeMillis() - d;
		System.err.println(all.get() + ", duration " + d + "ms");
		assertEquals(1000000, all.get());
	}

}
