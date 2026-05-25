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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import step.core.Version;
import step.core.collections.Collection;
import step.core.collections.CollectionFactory;
import step.core.collections.Document;
import step.core.collections.Filters;

public class MigrationManager {

    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    static final String MIGRATION_TASKS_STATUS_COLLECTION = "migrationTasksStatus";
    static final String STATUS_DOC_NAME_FIELD = "name";
    static final String STATUS_DOC_STATUS_FIELD = "status";

    private final List<Class<? extends MigrationTask>> migrationTasks = new ArrayList<>();
    private final Map<Class<?>, Object> bindings = new HashMap<>();

    public MigrationManager() {
        super();
    }

    public <T> void addBinding(Class<T> class_, T object) {
        bindings.put(class_, object);
    }

    /**
     * Register a new migration task (sync or async).
     */
    public void register(Class<? extends MigrationTask> migrationTask) {
        migrationTasks.add(migrationTask);
    }

    /**
     * Runs all synchronous migration tasks whose version falls in the [from, to] range.
     * {@link AsyncMigrationTask} instances are skipped here; submit them via {@link #migrateAsync}.
     *
     * @return true if every sync task ran without error
     */
    public boolean migrate(CollectionFactory collectionFactory, Version from, Version to) {
        boolean upgrade = to.compareTo(from) >= 1;
        logger.info("Migrating from {} to {}", from, to);
        AtomicBoolean successful = new AtomicBoolean(true);
        getMatchedMigrationTasks(collectionFactory, from, to, upgrade).stream()
                .filter(m -> !(m instanceof AsyncMigrationTask))
                .forEach(m -> {
                    logger.info("Running migration task {}", m);
                    long t1 = System.currentTimeMillis();
                    try {
                        if (upgrade) {
                            m.runUpgradeScript();
                        } else {
                            m.runDowngradeScript();
                        }
                        logger.info("Migration task {} successfully executed in {}ms", m, System.currentTimeMillis() - t1);
                    } catch (Exception e) {
                        logger.error("Error while running upgrade/downgrade script {}", m, e);
                        successful.set(false);
                    }
                });
        return successful.get();
    }

    /**
     * Submits async migration tasks to the provided executor.
     *
     * <p>Every registered {@link AsyncMigrationTask} whose {@code asOfVersion} is
     * {@code <= to} is a candidate. A task is submitted unless its status record in the
     * {@value #MIGRATION_TASKS_STATUS_COLLECTION} collection is already
     * {@link AsyncMigrationTaskStatus#COMPLETED}. This single rule covers all three situations:
     * <ul>
     *   <li><b>Initial upgrade</b> — task has no record yet ({@code asOfVersion} in {@code (from, to]}).</li>
     *   <li><b>Interrupted run</b> — task record exists with status {@link AsyncMigrationTaskStatus#STARTED}.</li>
     *   <li><b>Crash before status was written</b> — task has no record despite having been selected
     *       in a previous run; the absence of a record is treated the same as {@code STARTED}.</li>
     * </ul>
     * Downgrade ({@code to < from}) is not supported; all tasks with {@code asOfVersion <= to}
     * that are not {@code COMPLETED} will still be submitted.
     *
     * <p><b>Concurrency warning</b><br>
     * All matched tasks are submitted to the provided executor independently and may therefore
     * run in parallel when a multi-threaded executor is used. If any two registered async tasks
     * write to the same collection or shared resource, concurrent execution can produce conflicts
     * or data corruption. In that case, pass a single-threaded executor (e.g.
     * {@code Executors.newSingleThreadExecutor()}) to guarantee sequential execution.
     *
     * @param collectionFactory factory for the status collection
     * @param from              version before startup (used for logging)
     * @param to                current version; only tasks with {@code asOfVersion <= to} are candidates
     * @param executor          executor to which each task is submitted; use a single-threaded
     *                          executor if tasks share collections or other mutable resources
     */
    @SuppressWarnings("unchecked")
    public void migrateAsync(CollectionFactory collectionFactory, Version from, Version to, Executor executor) {
        Collection<Document> statusCollection = collectionFactory.getCollection(MIGRATION_TASKS_STATUS_COLLECTION, Document.class);

        List<AsyncMigrationTask> candidates;
        try (MigrationContext ctx = new MigrationContext()) {
            bindings.forEach((k, v) -> ctx.put((Class<Object>) k, v));
            candidates = migrationTasks.stream()
                    .map(m -> instantiateTask(m, collectionFactory, ctx))
                    .filter(t -> t instanceof AsyncMigrationTask)
                    .map(t -> (AsyncMigrationTask) t)
                    .filter(t -> t.asOfVersion.compareTo(to) <= 0)
                    .sorted((a, b) -> a.asOfVersion.compareTo(b.asOfVersion))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Error while creating migration context", e);
        }

        // Any candidate that is not COMPLETED must run: this covers new tasks (no record),
        // interrupted tasks (STARTED), and tasks whose status write was lost in a crash (no record).
        candidates.forEach(task -> {
            String taskName = task.getClass().getSimpleName();
            Document statusDoc = statusCollection.find(Filters.equals(STATUS_DOC_NAME_FIELD, taskName), null, null, null, 0)
                    .findFirst().orElse(new Document());
            if (AsyncMigrationTaskStatus.COMPLETED.name().equals(statusDoc.getString(STATUS_DOC_STATUS_FIELD))) {
                logger.debug("Async migration task {} already completed, skipping.", taskName);
                return;
            }
            statusDoc.put(STATUS_DOC_NAME_FIELD, taskName);
            statusDoc.put(STATUS_DOC_STATUS_FIELD, AsyncMigrationTaskStatus.STARTED.name());
            Document savedStatusDoc = statusCollection.save(statusDoc);
            executor.execute(() -> {
                logger.info("Running async migration task {}", taskName);
                long t1 = System.currentTimeMillis();
                try {
                    task.runAsyncUpgradeScript();
                    savedStatusDoc.put(STATUS_DOC_STATUS_FIELD, AsyncMigrationTaskStatus.COMPLETED.name());
                    statusCollection.save(savedStatusDoc);
                    logger.info("Async migration task {} completed in {}ms", taskName, System.currentTimeMillis() - t1);
                } catch (Exception e) {
                    logger.error("Error while running async migration task {}", taskName, e);
                }
            });
        });
    }

    @SuppressWarnings("unchecked")
    private List<MigrationTask> getMatchedMigrationTasks(CollectionFactory collectionFactory, Version from, Version to, boolean upgrade) {
        try (MigrationContext migrationContext = new MigrationContext()) {
            bindings.forEach((k, v) -> migrationContext.put((Class<Object>) k, v));
            List<MigrationTask> migrators = migrationTasks.stream()
                    .map(m -> instantiateTask(m, collectionFactory, migrationContext))
                    .collect(Collectors.toList());

            List<MigrationTask> matched = new ArrayList<>();
            for (MigrationTask migrator : migrators) {
                if (migrator.asOfVersion.compareTo(upgrade ? from : to) >= 1 && migrator.asOfVersion.compareTo(upgrade ? to : from) <= 0) {
                    matched.add(migrator);
                }
            }
            matched.sort((o1, o2) -> (upgrade ? 1 : -1) * o1.asOfVersion.compareTo(o2.asOfVersion));
            return matched;
        } catch (IOException e) {
            throw new RuntimeException("Error while creating migration context", e);
        }
    }

    private MigrationTask instantiateTask(Class<? extends MigrationTask> taskClass, CollectionFactory collectionFactory, MigrationContext ctx) {
        try {
            Constructor<? extends MigrationTask> constructor = taskClass.getConstructor(CollectionFactory.class, MigrationContext.class);
            return constructor.newInstance(collectionFactory, ctx);
        } catch (Exception e) {
            throw new RuntimeException("Error while creating instance of migration task " + taskClass.getName(), e);
        }
    }
}