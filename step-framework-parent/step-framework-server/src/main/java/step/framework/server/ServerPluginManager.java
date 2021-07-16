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

import ch.exense.commons.app.Configuration;
import step.core.plugins.PluginManager;
import step.core.plugins.PluginManager.Builder;
import step.core.plugins.PluginManager.Builder.CircularDependencyException;

public class ServerPluginManager {
	
	protected Configuration configuration;
	
	protected PluginManager<ServerPlugin> pluginManager;

	public ServerPluginManager(Configuration configuration) throws CircularDependencyException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		this.configuration = configuration;
		Builder<ServerPlugin> builder = new PluginManager.Builder<ServerPlugin>(ServerPlugin.class);
		this.pluginManager = builder.withPluginsFromClasspath().withPluginFilter(this::isPluginEnabled).build();
	}

	public ServerPlugin getProxy() {
		return pluginManager.getProxy(ServerPlugin.class);
	}

	private boolean isPluginEnabled(Object plugin) {
		return configuration.getPropertyAsBoolean("plugins." + plugin.getClass().getSimpleName() + ".enabled", true);
	}
}
