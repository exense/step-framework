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
package step.framework.server;

import step.core.AbstractContext;

/**
 * Lifecycle interface for server plugins. Implementations are discovered automatically via
 * classpath scanning and invoked by the {@link ServerPluginManager} through a proxy in a
 * well-defined startup/shutdown order.
 *
 * <p>The startup sequence is:
 * <ol>
 *   <li>{@link #bootstrapAndValidate(AbstractContext)} — earliest phase; used to validate
 *       environment requirements or register infrastructure objects (e.g. a
 *       {@link step.core.plugins.ModuleChecker}) into the context before the final plugin
 *       set is determined.</li>
 *   <li>{@link #init(AbstractContext)} — main initialization phase.</li>
 *   <li>{@link #recover(AbstractContext)} — recovery phase, e.g. after an unclean shutdown.</li>
 *   <li>{@link #serverStart(AbstractContext)} — called once the server is ready to start
 *       accepting work.</li>
 *   <li>{@link #migrateData(AbstractContext)} — data migration tasks.</li>
 *   <li>{@link #initializeData(AbstractContext)} — initial data population.</li>
 *   <li>{@link #afterInitializeData(AbstractContext)} — post data initialization scripts.</li>
 *   <li>{@link #finalizeStart(AbstractContext)} — final step of the startup sequence.</li>
 * </ol>
 *
 * <p>The shutdown sequence is:
 * <ol>
 *   <li>{@link #preShutdownHook(AbstractContext)} — called before the server begins shutting down.</li>
 *   <li>{@link #serverStop(AbstractContext)} — called while stopping the server.</li>
 *   <li>{@link #postShutdownHook()} — called after the server has shut down.</li>
 * </ol>
 *
 * <p>All methods have empty default implementations so that plugins only need to override the
 * phases they participate in.
 *
 * @param <C> the context type passed through the lifecycle
 */
public interface ServerPlugin<C extends AbstractContext> {

    /**
     * Earliest startup phase. Use this to validate preconditions or register infrastructure
     * objects into the context (e.g. a {@link step.core.plugins.ModuleChecker}) that must be
     * available before the final set of enabled plugins is determined.
     */
    default void bootstrapAndValidate(C context) throws Exception {
    }

    /**
     * Main initialization phase, called after preconditions have been checked and the active
     * plugin set has been finalized.
     */
    default void init(C context) throws Exception {
    }

    /**
     * Recovery phase, called after {@link #init(AbstractContext)}, e.g. to handle state left
     * over from an unclean shutdown.
     */
    default void recover(C context) throws Exception {
    }

    /**
     * Called once the server is ready to start accepting work.
     */
    default void serverStart(C context) throws Exception {
    }

    /**
     * Data migration tasks, executed after {@link #serverStart(AbstractContext)}.
     */
    default void migrateData(C context) throws Exception {
    }

    /**
     * Initial data population, executed after {@link #migrateData(AbstractContext)}.
     */
    default void initializeData(C context) throws Exception {
    }

    /**
     * Post data initialization scripts, executed after {@link #initializeData(AbstractContext)}.
     */
    default void afterInitializeData(C context) throws Exception {
    }

    /**
     * Final step of the startup sequence.
     */
    default void finalizeStart(C context) throws Exception {
    }

    /**
     * Called before the server begins shutting down.
     */
    default void preShutdownHook(C context) {
    }

    /**
     * Called when stopping the controller
     */
    default void serverStop(C context) {
    }

    /**
     * Called after the server has shut down.
     */
    default void postShutdownHook() {
    }

    /**
     * Returns {@code false} if this plugin is mandatory and must not be disabled via
     * configuration or module filtering. Defaults to {@code true}.
     */
    default boolean canBeDisabled() {
        return true;
    }

}
