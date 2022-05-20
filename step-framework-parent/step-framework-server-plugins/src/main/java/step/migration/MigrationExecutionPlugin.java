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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import step.core.AbstractContext;
import step.core.Version;
import step.core.collections.CollectionFactory;
import step.core.plugins.Plugin;
import step.framework.server.ServerPlugin;
import step.versionmanager.ControllerLog;
import step.versionmanager.VersionManager;

@Plugin
/**
 * This plugin is responsible for the execution of the Migration Tasks
 */
public class MigrationExecutionPlugin<C extends AbstractContext> implements ServerPlugin<C> {

	private static final Logger logger = LoggerFactory.getLogger(MigrationExecutionPlugin.class);

	@Override
	public void serverStart(C context) throws Exception {

	}

	@Override
	public void migrateData(C context) throws Exception {
		checkVersion(context);	
	}

	@Override
	public void initializeData(C context) throws Exception {

	}

	@Override
	public void afterInitializeData(C context) throws Exception {

	}

	@Override
	public void serverStop(C context) {

	}

	@Override
	public boolean canBeDisabled() {
		return false;
	}

	private void checkVersion(C context) {
		MigrationManager migrationManager = context.get(MigrationManager.class);
		VersionManager versionManager = context.get(VersionManager.class);
		
		ControllerLog latestLog = versionManager.getLatestControllerLog();
		if(latestLog!=null) {
			Version latestVersion = latestLog.getVersion();
			// Version tracking has been introduced with 3.8.0 therefore assuming version 3.7.0 as latest version if null
			if(latestVersion==null) {
				latestVersion = new Version(3, 7, 0);
			}
			Version currentVersion = context.require(Version.class);
			if(currentVersion.compareTo(latestVersion)>0) {
				logger.info("Starting controller with a newer version. Current version is "
						+ currentVersion +". Version of last start was "+latestVersion);
			} else if (currentVersion.compareTo(latestVersion)<0) {
				logger.info("Starting controller with an older version. Current version is "
						+ currentVersion +". Version of last start was "+latestVersion);
			}
			migrationManager.migrate(context.get(CollectionFactory.class), latestVersion, currentVersion);
		}					
	}


}
