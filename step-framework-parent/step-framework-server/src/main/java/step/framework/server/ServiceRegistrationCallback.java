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

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import java.util.EnumSet;

public interface ServiceRegistrationCallback {

	public void register(Object component);

	public void registerService(Class<?> serviceClass);

	public void registerHandler(Handler handler);

	public void registerServlet(ServletHolder servletHolder, String subPath);

	public FilterHolder registerServletFilter(Class<? extends Filter> filterClass, String pathSpec, EnumSet<DispatcherType> dispatches);

	public void stop();

	public void registerPackage(Package aPackage);
}
