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
import step.core.plugins.ModuleChecker;
import step.core.plugins.PluginManager;
import step.core.plugins.PluginManager.Builder;
import step.core.plugins.PluginManager.Builder.CircularDependencyException;
import step.core.plugins.exceptions.PluginCriticalException;

import java.util.List;
import java.util.stream.Collectors;

public class ServerPluginManager<P extends ServerPlugin> {
	
	protected Configuration configuration;

	protected ModuleChecker moduleChecker;
	
	protected PluginManager<ServerPlugin> pluginManager;

	public ServerPluginManager(Configuration configuration, ModuleChecker moduleChecker) throws CircularDependencyException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		this.configuration = configuration;
		this.moduleChecker = moduleChecker;
		Builder<ServerPlugin> builder = new PluginManager.Builder<ServerPlugin>(ServerPlugin.class);
		this.pluginManager = builder.withPluginsFromClasspath().withPluginFilter(this::isPluginEnabled).build();
	}

	public ServerPlugin getProxy() {
		return pluginManager.getProxy(ServerPlugin.class);
	}

	protected boolean isPluginEnabled(ServerPlugin plugin) {
		String pluginName = plugin.getClass().getSimpleName();
		boolean enabled = configuration.getPropertyAsBoolean("plugins." + pluginName + ".enabled", true)
				&& (moduleChecker == null || moduleChecker.apply(plugin));
		if (!enabled && !plugin.canBeDisabled()) {
			throw new PluginCriticalException("The plugin " + pluginName + " cannot be disabled");
		}
		return enabled;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public PluginManager<ServerPlugin> getPluginManager() {
		return pluginManager;
	}


	public PluginManager<P> cloneAs(Class<P> pluginClass) throws CircularDependencyException {
		PluginManager.Builder<P> builder = new PluginManager.Builder(pluginClass );
		List<P> collect = this.getPluginManager().getPlugins().stream().filter(pluginClass::isInstance).map(pluginClass::cast).collect(Collectors.toList());
		 return builder.withPlugins(collect).build();
	}
}
