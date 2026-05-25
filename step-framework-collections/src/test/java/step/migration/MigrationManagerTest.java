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
package step.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import step.core.Version;
import step.core.collections.Collection;
import step.core.collections.CollectionFactory;
import step.core.collections.Document;
import step.core.collections.Filters;
import step.core.collections.inmemory.InMemoryCollectionFactory;

public class MigrationManagerTest {

    private StringBuilder s;

    @Before
    public void before() {
        s = new StringBuilder();
    }

    // -------------------------------------------------------------------------
    // Sync migration tests (unchanged behaviour)
    // -------------------------------------------------------------------------

    @Test
    public void testUpgrade() {
        MigrationManager m = getMigrationManager();
        m.migrate(null, new Version(1, 2, 2), new Version(1, 2, 4));
        assertEquals("1.2.3,1.2.4,", s.toString());
    }

    @Test
    public void testUpgrade2() {
        MigrationManager m = getMigrationManager();
        m.migrate(null, new Version(1, 2, 2), new Version(5, 2, 4));
        assertEquals("1.2.3,1.2.4,1.2.5,2.2.5,", s.toString());
    }

    @Test
    public void testDowngrade() {
        MigrationManager m = getMigrationManager();
        m.migrate(null, new Version(1, 2, 4), new Version(1, 2, 2));
        assertEquals("-1.2.4,-1.2.3,", s.toString());
    }

    // -------------------------------------------------------------------------
    // Async migration tests
    // -------------------------------------------------------------------------

    /**
     * Async tasks must be invisible to the synchronous migrate() path.
     * The sync task at 2.2.5 still runs; the async task at 3.0.0 must not appear.
     */
    @Test
    public void testAsyncTaskSkippedBySyncMigrate() {
        MigrationManager m = getMigrationManager();
        m.migrate(null, new Version(2, 0, 0), new Version(3, 0, 0));
        assertEquals("2.2.5,", s.toString());
    }

    /**
     * Async task runs when its version falls in the upgrade range.
     */
    @Test
    public void testAsyncTaskRunsDuringUpgrade() {
        CollectionFactory cf = newCollectionFactory();
        MigrationManager m = getMigrationManager();
        m.migrateAsync(cf, new Version(2, 0, 0), new Version(3, 0, 0), Runnable::run);
        assertEquals("async-3.0.0,", s.toString());
        assertStatus(cf, "AsyncTestMigrationTask_3_0_0", AsyncMigrationTaskStatus.COMPLETED);
    }

    /**
     * A task registered for a future version (> to) is never run.
     */
    @Test
    public void testAsyncTaskForFutureVersionNotRun() {
        CollectionFactory cf = newCollectionFactory();
        MigrationManager m = getMigrationManager();
        // to = 2.0.0 < asOfVersion 3.0.0 → must not run
        m.migrateAsync(cf, new Version(1, 0, 0), new Version(2, 0, 0), Runnable::run);
        assertEquals("", s.toString());
        assertNoStatus(cf, "AsyncTestMigrationTask_3_0_0");
    }

    /**
     * An already-COMPLETED task is never re-submitted.
     */
    @Test
    public void testAsyncTaskSkippedIfAlreadyCompleted() {
        CollectionFactory cf = newCollectionFactory();
        seedStatus(cf, "AsyncTestMigrationTask_3_0_0", AsyncMigrationTaskStatus.COMPLETED);
        MigrationManager m = getMigrationManager();
        m.migrateAsync(cf, new Version(2, 0, 0), new Version(3, 0, 0), Runnable::run);
        assertEquals("", s.toString());
    }

    /**
     * Core regression test: when the server restarts at the same version (from == to),
     * a task that was STARTED but never COMPLETED must be re-submitted.
     */
    @Test
    public void testAsyncTaskResumedAfterRestartAtSameVersion() {
        CollectionFactory cf = newCollectionFactory();
        seedStatus(cf, "AsyncTestMigrationTask_3_0_0", AsyncMigrationTaskStatus.STARTED);
        MigrationManager m = getMigrationManager();
        // Restart: from == to == 3.0.0; task is a candidate (asOfVersion <= to) and not COMPLETED
        m.migrateAsync(cf, new Version(3, 0, 0), new Version(3, 0, 0), Runnable::run);
        assertEquals("async-3.0.0,", s.toString());
        assertStatus(cf, "AsyncTestMigrationTask_3_0_0", AsyncMigrationTaskStatus.COMPLETED);
    }

    /**
     * A task whose STARTED record was never written (crash before the DB write) is resumed
     * because the absence of a COMPLETED record is sufficient to re-submit it.
     */
    @Test
    public void testAsyncTaskResumedWhenNoRecordExists() {
        CollectionFactory cf = newCollectionFactory();
        // No status record at all — simulates a crash before the STARTED write
        MigrationManager m = getMigrationManager();
        m.migrateAsync(cf, new Version(3, 0, 0), new Version(3, 0, 0), Runnable::run);
        assertEquals("async-3.0.0,", s.toString());
        assertStatus(cf, "AsyncTestMigrationTask_3_0_0", AsyncMigrationTaskStatus.COMPLETED);
    }

    /**
     * A task that was STARTED and interrupted runs again on a subsequent upgrade
     * even when its version is below the new upgrade range.
     */
    @Test
    public void testAsyncTaskResumedWhenBelowCurrentUpgradeRange() {
        CollectionFactory cf = newCollectionFactory();
        seedStatus(cf, "AsyncTestMigrationTask_3_0_0", AsyncMigrationTaskStatus.STARTED);
        MigrationManager m = getMigrationManager();
        // Upgrading from 3.0.0 to 4.0.0 — task at 3.0.0 is still a candidate (asOfVersion <= to)
        m.migrateAsync(cf, new Version(3, 0, 0), new Version(4, 0, 0), Runnable::run);
        assertEquals("async-3.0.0,", s.toString());
        assertStatus(cf, "AsyncTestMigrationTask_3_0_0", AsyncMigrationTaskStatus.COMPLETED);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private MigrationManager getMigrationManager() {
        MigrationManager m = new MigrationManager();
        m.addBinding(StringBuilder.class, s);
        m.register(TestMigrationTask_0_2_2.class);
        m.register(TestMigrationTask_2_2_5.class);
        m.register(TestMigrationTask_1_2_2.class);
        m.register(TestMigrationTask_1_2_3.class);
        m.register(TestMigrationTask_1_2_4.class);
        m.register(TestMigrationTask_1_2_5.class);
        m.register(AsyncTestMigrationTask_3_0_0.class);
        return m;
    }

    private static CollectionFactory newCollectionFactory() {
        return new InMemoryCollectionFactory(new Properties());
    }

    private static void seedStatus(CollectionFactory cf, String taskName, AsyncMigrationTaskStatus status) {
        Collection<Document> col = cf.getCollection(MigrationManager.MIGRATION_TASKS_STATUS_COLLECTION, Document.class);
        Document doc = new Document();
        doc.put(MigrationManager.STATUS_DOC_NAME_FIELD, taskName);
        doc.put(MigrationManager.STATUS_DOC_STATUS_FIELD, status.name());
        col.save(doc);
    }

    private static void assertStatus(CollectionFactory cf, String taskName, AsyncMigrationTaskStatus expected) {
        Collection<Document> col = cf.getCollection(MigrationManager.MIGRATION_TASKS_STATUS_COLLECTION, Document.class);
        Document doc = col.find(Filters.equals(MigrationManager.STATUS_DOC_NAME_FIELD, taskName), null, null, null, 0)
            .findFirst().orElse(null);
        assertNotNull("No status record for " + taskName, doc);
        assertEquals(expected.name(), doc.getString(MigrationManager.STATUS_DOC_STATUS_FIELD));
    }

    private static void assertNoStatus(CollectionFactory cf, String taskName) {
        Collection<Document> col = cf.getCollection(MigrationManager.MIGRATION_TASKS_STATUS_COLLECTION, Document.class);
        Document doc = col.find(Filters.equals(MigrationManager.STATUS_DOC_NAME_FIELD, taskName), null, null, null, 0)
            .findFirst().orElse(null);
        assertNull("Expected no status record for " + taskName + " but found one", doc);
    }

    // -------------------------------------------------------------------------
    // Sync task implementations
    // -------------------------------------------------------------------------

    private abstract static class TestMigrationTask extends MigrationTask {

        private final StringBuilder s;

        public TestMigrationTask(Version asOfVersion, CollectionFactory collectionFactory,
                                 MigrationContext migrationContext) {
            super(asOfVersion, collectionFactory, migrationContext);
            s = migrationContext.require(StringBuilder.class);
        }

        @Override
        public void runUpgradeScript() {
            s.append(asOfVersion.toString()).append(",");
        }

        @Override
        public void runDowngradeScript() {
            s.append("-").append(asOfVersion.toString()).append(",");
        }
    }

    private static class TestMigrationTask_0_2_2 extends TestMigrationTask {
        public TestMigrationTask_0_2_2(CollectionFactory cf, MigrationContext ctx) {
            super(new Version(0, 2, 2), cf, ctx);
        }
    }

    private static class TestMigrationTask_2_2_5 extends TestMigrationTask {
        public TestMigrationTask_2_2_5(CollectionFactory cf, MigrationContext ctx) {
            super(new Version(2, 2, 5), cf, ctx);
        }
    }

    private static class TestMigrationTask_1_2_2 extends TestMigrationTask {
        public TestMigrationTask_1_2_2(CollectionFactory cf, MigrationContext ctx) {
            super(new Version(1, 2, 2), cf, ctx);
        }
    }

    private static class TestMigrationTask_1_2_3 extends TestMigrationTask {
        public TestMigrationTask_1_2_3(CollectionFactory cf, MigrationContext ctx) {
            super(new Version(1, 2, 3), cf, ctx);
        }
    }

    private static class TestMigrationTask_1_2_4 extends TestMigrationTask {
        public TestMigrationTask_1_2_4(CollectionFactory cf, MigrationContext ctx) {
            super(new Version(1, 2, 4), cf, ctx);
        }
    }

    private static class TestMigrationTask_1_2_5 extends TestMigrationTask {
        public TestMigrationTask_1_2_5(CollectionFactory cf, MigrationContext ctx) {
            super(new Version(1, 2, 5), cf, ctx);
        }
    }

    // -------------------------------------------------------------------------
    // Async task implementation
    // -------------------------------------------------------------------------

    public static class AsyncTestMigrationTask_3_0_0 extends AsyncMigrationTask {

        private final StringBuilder s;

        public AsyncTestMigrationTask_3_0_0(CollectionFactory cf, MigrationContext ctx) {
            super(new Version(3, 0, 0), cf, ctx);
            s = ctx.require(StringBuilder.class);
        }

        @Override
        public void runAsyncUpgradeScript() {
            s.append("async-").append(asOfVersion.toString()).append(",");
        }
    }
}
