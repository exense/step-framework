package step.migration;

import step.core.Version;
import step.core.collections.CollectionFactory;

/**
 * A migration task whose data-intensive work is performed asynchronously after startup,
 * outside the synchronous migration window.
 *
 * <p><b>Execution model</b><br>
 * Unlike {@link MigrationTask}, an {@code AsyncMigrationTask} is submitted to a caller-supplied
 * {@link java.util.concurrent.Executor} by {@link MigrationManager#migrateAsync}. Depending on
 * the executor, multiple async tasks may run <em>concurrently</em>. If two tasks touch the same
 * collection or shared resource, their concurrent execution can produce conflicts or data
 * corruption. In that case the caller must use a single-threaded executor (e.g.
 * {@code Executors.newSingleThreadExecutor()}) to enforce sequential execution.
 *
 * <p><b>Restart / resume semantics</b><br>
 * The migration manager tracks each task's progress in a persistent status collection.
 * A task is re-submitted on the next startup whenever its status is
 * {@link AsyncMigrationTaskStatus#STARTED} but not yet {@link AsyncMigrationTaskStatus#COMPLETED}
 * — for example after a server crash or a clean shutdown that occurred mid-migration.
 * Implementations of {@link #runAsyncUpgradeScript()} must therefore be written so that
 * re-running them from scratch on an already partially-migrated dataset is safe (i.e.
 * the operation must be idempotent or explicitly skip already-processed records).
 */
public abstract class AsyncMigrationTask extends MigrationTask {

    public AsyncMigrationTask(Version asOfVersion, CollectionFactory collectionFactory, MigrationContext migrationContext) {
        super(asOfVersion, collectionFactory, migrationContext);
    }

    @Override
    public final void runUpgradeScript() {
        // Async tasks are not executed by the synchronous migration loop
    }

    /**
     * Performs the asynchronous migration work for this task.
     *
     * <p><b>Idempotency requirement</b><br>
     * This method may be called more than once on the same dataset if the process was interrupted
     * (crash, shutdown) before the previous run completed. Implementations must be safe to re-run
     * from scratch, for example by checking whether each record was already migrated before
     * modifying it (e.g. testing for the presence of the new field, or using an upsert).
     *
     * <p><b>Concurrency</b><br>
     * If the caller supplies a multi-threaded executor to {@link MigrationManager#migrateAsync},
     * this method may execute concurrently with other async migration tasks. Implementations that
     * share a collection with another async task must either be concurrency-safe or the caller
     * must enforce sequential execution via a single-threaded executor.
     */
    public abstract void runAsyncUpgradeScript();
}