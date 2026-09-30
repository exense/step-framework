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
package step.versionmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.AbstractContext;
import step.core.Version;
import step.core.collections.*;

import java.util.Date;
import java.util.Objects;
import java.util.Optional;

public class VersionManager<C extends AbstractContext> {

    private static final Logger logger = LoggerFactory.getLogger(VersionManager.class);

    /** Version tracking was introduced in 3.8.0, so a start recorded before it carries no version. */
    public static final Version VERSION_BEFORE_VERSION_TRACKING = new Version(3, 7, 0);

    private final C context;

    private final Version currentVersion;

    private final Collection<ControllerLog> controllerLogs;

    private ControllerLog latestControllerLog = null;

    public VersionManager(C context) {
        super();
        this.context = context;
        this.currentVersion = context.require(Version.class);
        controllerLogs = context.require(CollectionFactory.class).getCollection("controllerlogs", ControllerLog.class);
    }

    public void readLatestControllerLog() {
        latestControllerLog = controllerLogs.find(Filters.empty(), new SearchOrder("start", -1), null, null, 0).findFirst().orElse(null);

        if (latestControllerLog != null) {
            logger.info("Last start of the controller: " + latestControllerLog.toString());
        } else {
            logger.info("No start log found. Starting the controller for the first time against this DB...");
        }
    }

    public ControllerLog getLatestControllerLog() {
        return latestControllerLog;
    }

    /**
     * The version the data in the database was last used with, which is what the migration tasks of
     * this start are selected against. Read before the current start is recorded, so it stays available
     * for the whole startup.
     *
     * @return the version of the previous start, {@link #VERSION_BEFORE_VERSION_TRACKING} when that
     * start predates version tracking, or empty when this is the first start against this database
     */
    public Optional<Version> getPreviousVersion() {
        return Optional.ofNullable(latestControllerLog)
                .map(log -> Objects.requireNonNullElse(log.getVersion(), VERSION_BEFORE_VERSION_TRACKING));
    }

    public void setLatestControllerLog(ControllerLog latestControllerLog) {
        this.latestControllerLog = latestControllerLog;
    }

    public void insertControllerLog() {
        ControllerLog logEntry = new ControllerLog();
        logEntry.setStart(new Date());
        logEntry.setVersion(this.currentVersion);
        controllerLogs.save(logEntry);
    }
}
