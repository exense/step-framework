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
import step.core.plugins.Plugin;
import step.framework.server.ServerPlugin;

@Plugin()
public class VersionManagerPlugin<C extends AbstractContext> implements ServerPlugin<C> {

	private static final Logger logger = LoggerFactory.getLogger(VersionManagerPlugin.class);

	@Override
	public void serverStart(C context) throws Exception {
		VersionManager versionManager = new VersionManager(context);
		context.put(VersionManager.class, versionManager);

		versionManager.readLatestControllerLog();
		versionManager.insertControllerLog();
	}

	@Override
	public void migrateData(C context) throws Exception {

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
		return true;
	}
}
