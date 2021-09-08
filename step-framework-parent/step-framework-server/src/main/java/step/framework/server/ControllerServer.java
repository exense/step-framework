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
import java.util.logging.LogManager;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.exense.commons.app.ArgumentParser;
import ch.exense.commons.app.Configuration;


public class ControllerServer {

	private Configuration configuration;
	
	private Server server;
	
	private ContextHandlerCollection handlers;
	
	private Integer port;
	
	private static final Logger logger = LoggerFactory.getLogger(ControllerServer.class);

	private ServerPlugin pluginProxy;

	private ServerContext serverContext;
	
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
		//SLF4JBridgeHandler.install();
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
			server.stop();
		} catch (Exception e) {
			logger.error("Error while stopping jetty",e);
		} finally {
			server.destroy();
		}
		if(configuration != null) {
			try {
				configuration.close();
			} catch (IOException e) {
				logger.error("Error while closing configuration",e);
			}
		}
		if(pluginProxy != null && serverContext != null) {
			pluginProxy.serverStop(serverContext);
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
			
			SslContextFactory sslContextFactory = new SslContextFactory();
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
		ResourceHandler bb = new ResourceHandler();
		//bb.setResourceBase(Resource.newClassPathResource("webapp").getURI().toString());
		//bb.setEtags(true);

		ContextHandler ctx = new ContextHandler("/"); /* the server uri path */
		ctx.setHandler(bb);

		addHandler(ctx);
	}

	private void initController() throws Exception {
		ResourceConfig resourceConfig = new ResourceConfig();
		//resourceConfig.packages(ControllerServices.class.getPackage().getName());

		ServiceRegistrationCallback serviceRegistrationCallback = new ServiceRegistrationCallback() {
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
			public void stop() {
				try {
					ControllerServer.this.stop();
				} catch (Exception e) {
					logger.error("Error while trying to stop the controller",e);
				}
			}
		};

		resourceConfig.register(JacksonMapperProvider.class);
		
		serverContext = new ServerContext();
		serverContext.put(ServiceRegistrationCallback.class, serviceRegistrationCallback);
		serverContext.put(Configuration.class, configuration);
		ServerPluginManager serverPluginManager = new ServerPluginManager(configuration);
		
		pluginProxy = serverPluginManager.getProxy();
		pluginProxy.serverStart(serverContext);
		pluginProxy.initializeData(serverContext);
		pluginProxy.afterInitializeData(serverContext);
		
		// Enabling CORS. Required for tests only
		resourceConfig.register(CORSResponseFilter.class);
		
//		resourceConfig.register(ObjectMapperProvider.class);
		//resourceConfig.registerClasses(SecurityFilter.class);
		
		resourceConfig.register(new AbstractBinder() {	
			@Override
			protected void configure() {
				bind(serverContext).to(ServerContext.class);
//				bindFactory(HttpSessionFactory.class).to(HttpSession.class)
//                .proxy(true).proxyForSameScope(false).in(RequestScoped.class);
			}
		});
		
		ServletContainer servletContainer = new ServletContainer(resourceConfig);

		ServletHolder sh = new ServletHolder(servletContainer);
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/rest");
		context.addServlet(sh, "/*");
		
		SessionHandler s = new SessionHandler();
		Integer timeout = configuration.getPropertyAsInteger("ui.sessiontimeout.minutes", 180)*60;
		s.setMaxInactiveInterval(timeout);
        s.setUsingCookies(true);
        s.setSessionCookie("sessionid");
		context.setSessionHandler(s);
        
		addHandler(context);
	}
	
	private synchronized void addHandler(Handler handler) {
		handlers.addHandler(handler);
	}
	
	public interface ServiceRegistrationCallback {

		public void register(Object component);
		
		public void registerService(Class<?> serviceClass);
		
		public void registerHandler(Handler handler);
		
		public void stop();
	}
}
