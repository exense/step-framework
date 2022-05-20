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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.logging.LogManager;

import org.eclipse.jetty.http.HttpCookie;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import ch.exense.commons.app.ArgumentParser;
import ch.exense.commons.app.Configuration;
import step.core.AbstractContext;
import step.core.OverrideServerContext;
import step.core.plugins.ModuleChecker;
import step.core.plugins.PluginManager;
import step.core.scanner.CachedAnnotationScanner;
import step.framework.server.audit.AuditLogger;
import step.framework.server.swagger.Swagger;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;


public class ControllerServer {

	private Configuration configuration;
	
	private Server server;
	
	private ContextHandlerCollection handlers;
	
	private Integer port;
	
	private static final Logger logger = LoggerFactory.getLogger(ControllerServer.class);

	private ServerPlugin pluginProxy;

	ControllerInitializationPlugin initPluginProxy;

	private AbstractContext serverContext;

	ServletContextHandler servletContextHandler;

	public static void main(String[] args) throws Exception {
		ArgumentParser arguments = new ArgumentParser(args);
		
		Configuration configuration; 
		String configStr = arguments.getOption("config");
		if(configStr!=null) {
			configuration = new Configuration(new File(configStr), arguments.getOptions());
		} else {
			configuration = new Configuration();
		}
		
		arguments.entrySet().forEach(e->configuration.putProperty(e.getKey(),e.getValue()));
		
		setupLogging();
		
		(new ControllerServer(configuration)).start();
	}

	protected static void setupLogging() {
		LogManager.getLogManager().reset();
		SLF4JBridgeHandler.install(); //used by step
	}
	
	public ControllerServer(Configuration configuration) {
		super();
		this.configuration = configuration;
		this.port = configuration.getPropertyAsInteger("port", 8080);
	}

	public void start() throws Exception {
		server = new Server();
		handlers = new ContextHandlerCollection();

		initController();
		initWebapp();
		
		setupConnectors();

		server.setHandler(handlers);
		server.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("Shutdown hook called. Stopping...");
			try {
				stop();
			} catch (Exception e) {
				logger.error("Unexpected error while stopping server", e);
			}
		}));
	}
	
	private void stop() {
		try {
			initPluginProxy.preShutdownHook(serverContext);
		} catch (Exception e) {
			logger.error("Error while calling plugin pre-shutdown hooks");
		}
		try {
			server.stop();
		} catch (Exception e) {
			logger.error("Error while stopping jetty",e);
		} finally {
			server.destroy();
		}
		if(pluginProxy != null && serverContext != null) {
			pluginProxy.serverStop(serverContext);
		}
		if(configuration != null) {
			try {
				configuration.close();
			} catch (IOException e) {
				logger.error("Error while closing configuration",e);
			}
		}

		if(serverContext != null) {
			try {
				serverContext.close();
			} catch (IOException e) {
				logger.error("Error while closing serverContext",e);
			}
		}
	}

	private void setupConnectors() {
		HttpConfiguration http = new HttpConfiguration();
		http.addCustomizer(new SecureRequestCustomizer());
		http.setSecureScheme("https");

		ServerConnector connector = new ServerConnector(server);
		connector.addConnectionFactory(new HttpConnectionFactory(http));
		connector.setPort(port);
		
		if(configuration.getPropertyAsBoolean("ui.ssl.enabled", false)) {
			int httpsPort = configuration.getPropertyAsInteger("ui.ssl.port", 443);
			
			http.setSecurePort(httpsPort);

			HttpConfiguration https = new HttpConfiguration();
			https.addCustomizer(new SecureRequestCustomizer());
			
			SslContextFactory sslContextFactory = new SslContextFactory.Server();
			sslContextFactory.setKeyStorePath(configuration.getProperty("ui.ssl.keystore.path"));
			sslContextFactory.setKeyStorePassword(configuration.getProperty("ui.ssl.keystore.password"));
			sslContextFactory.setKeyManagerPassword(configuration.getProperty("ui.ssl.keymanager.password"));
			
			ServerConnector sslConnector = new ServerConnector(server, new SslConnectionFactory(sslContextFactory, "http/1.1"), new HttpConnectionFactory(https));
			sslConnector.setPort(httpsPort);
			server.addConnector(sslConnector);
		}

		server.addConnector(connector);
	}

	private void initWebapp() throws Exception {
		ResourceCollection resources = new ResourceCollection(
				Resource.newClassPathResource(configuration.getProperty("ui.resource.root","dist/step-app")),
				Resource.newClassPathResource("webapp"));
		servletContextHandler.setBaseResource(resources);
		servletContextHandler.setContextPath("/");
		addHandler(servletContextHandler);
	}

	private void initController() throws Exception {
		ResourceConfig resourceConfig = new ResourceConfig();
		resourceConfig.addProperties(Map.of("produces", Arrays.asList("application/json")));
		resourceConfig.addProperties(Map.of("consumes", Arrays.asList("application/json")));

		// Enabling CORS. Required for tests only
		resourceConfig.register(CORSRequestResponseFilter.class);
		resourceConfig.register(MultiPartFeature.class);
		resourceConfig.register(new AbstractBinder() {
			@Override
			protected void configure() {
				//C extends AbstractContext
				//bindAsContract(GenericType.forInstance(serverContext));
				//bind(serverContext).to(GenericType.forInstance(serverContext));
				bind(serverContext).to(AbstractContext.class);
				bind(serverContext).to(serverContext.getClass());
				bind(configuration).to(Configuration.class);
			}
		});

		servletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
		ServletContainer servletContainer = new ServletContainer(resourceConfig);
		ServletHolder sh = new ServletHolder(servletContainer);
		servletContextHandler.addServlet(sh, "/rest/*");


		ServiceRegistrationCallback serviceRegistrationCallback = new ServiceRegistrationCallbackImpl(resourceConfig, servletContextHandler);

		resourceConfig.register(JacksonMapperProvider.class);

		serverContext = getServerContext();
		serverContext.put(ServiceRegistrationCallback.class, serviceRegistrationCallback);
		serverContext.put(Configuration.class, configuration);

		//Manage plugins
		ServerPluginManager serverPluginManager = new ServerPluginManager(configuration);
		serverContext.put(ServerPluginManager.class, serverPluginManager);
		pluginProxy = serverPluginManager.getProxy();

		//Initialization plugins check preconditions and recover
		PluginManager<ControllerInitializationPlugin> initPluginManager = serverPluginManager.cloneAs(ControllerInitializationPlugin.class);
		initPluginProxy = initPluginManager.getProxy();
		logger.info("Checking preconditions...");
		initPluginProxy.checkPreconditions(serverContext);
		logger.info("Recovering controller...");
		initPluginProxy.recover(serverContext);

		//module checker is created during by initalization plugins, set it now to the pluginManager
		ModuleChecker moduleChecker = serverContext.get(ModuleChecker.class);
		serverPluginManager.setModuleChecker(moduleChecker);

		//start all plugins and init data
		logger.info("Starting controller...");
		pluginProxy.serverStart(serverContext);
		logger.info("Executing migration tasks...");
		pluginProxy.migrateData(serverContext);
		logger.info("Initializing data...");
		pluginProxy.initializeData(serverContext);
		logger.info("Calling post data initialization scripts...");
		pluginProxy.afterInitializeData(serverContext);

		//Initialization plugins cal final steps
		initPluginProxy.finalizeStart(serverContext);

		//Http session management
		SessionHandler s = new SessionHandler();
		Integer timeout = configuration.getPropertyAsInteger("ui.sessiontimeout.minutes", 180)*60;
		s.setMaxInactiveInterval(timeout);
		s.setUsingCookies(true);
		s.setSessionCookie("sessionid");
		s.setSameSite(HttpCookie.SameSite.LAX);
		s.setHttpOnly(true);
		servletContextHandler.setSessionHandler(s);
		servletContextHandler.addEventListener(new HttpSessionListener() {
			@Override
			public void sessionCreated(HttpSessionEvent httpSessionEvent) {}
			@Override
			public void sessionDestroyed(HttpSessionEvent httpSessionEvent) {
				AuditLogger.logSessionInvalidation(httpSessionEvent.getSession());
			}
		});

		Swagger.setup(resourceConfig, serverContext);

		// Lastly, the default servlet for root content (always needed, to satisfy servlet spec)
		// It is important that this is last.
		ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);
		holderPwd.setInitParameter("dirAllowed","true");
		servletContextHandler.addServlet(holderPwd,"/");

		addHandler(servletContextHandler);
	}

	private AbstractContext getServerContext() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		Set<Class<?>> classesWithAnnotation = CachedAnnotationScanner.getClassesWithAnnotation(OverrideServerContext.class);
		if (classesWithAnnotation.size() > 0) {
			Class<?> aClass = classesWithAnnotation.stream().findFirst().get();
			if (classesWithAnnotation.size() > 1) {
				logger.warn("Multiple classes using @OverrideServerContext found, selecting first one as main Context class: " + classesWithAnnotation);
			} else {
				logger.info("Using " + aClass.getName() + " as server context");
			}
			if (AbstractContext.class.isAssignableFrom(aClass)) {
				return (AbstractContext) aClass.getConstructor().newInstance();
			} else {
				logger.error("Class '" + aClass.getName() + "' annotated with OverrideServerContext is not assignable, reverting to default ServerContext class");
			}
		}
		logger.info("Using default server context class");
		return new ServerContext();
	}
	
	private synchronized void addHandler(Handler handler) {
		handlers.addHandler(handler);
	}

	private class ServiceRegistrationCallbackImpl implements ServiceRegistrationCallback {
		private final ResourceConfig resourceConfig;
		private final ServletContextHandler context;

		public ServiceRegistrationCallbackImpl(ResourceConfig resourceConfig, ServletContextHandler context) {
			this.resourceConfig = resourceConfig;
			this.context = context;
		}

		@Override
		public void register(Object component) {
			resourceConfig.register(component);
		}

		public void registerService(Class<?> serviceClass) {
			resourceConfig.registerClasses(serviceClass);
		}

		@Override
		public void registerHandler(Handler handler) {
			addHandler(handler);
		}

		@Override
		public void registerServlet(ServletHolder servletHolder, String subPath) {
			context.addServlet(servletHolder, subPath);
		}

		@Override
		public FilterHolder registerServletFilter(Class<? extends Filter> filterClass, String pathSpec, EnumSet<DispatcherType> dispatches) {
			return context.addFilter(filterClass, pathSpec, dispatches);
		}

		@Override
		public void stop() {
			try {
				ControllerServer.this.stop();
			} catch (Exception e) {
				logger.error("Error while trying to stop the controller",e);
			}
		}

		@Override
		public void registerPackage(Package aPackage) {
			resourceConfig.packages(aPackage.getName());
		}
	}
}
