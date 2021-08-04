package step.framework.server;

import ch.exense.commons.app.Configuration;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

@Provider
public class CORSResponseFilter extends AbstractServices implements ContainerResponseFilter {

	private String origin;
	
	@PostConstruct
	public void init() throws Exception {
		Configuration configuration = getServerContext().getConfiguration();
		origin = configuration.getProperty("frontend.baseUrl", "*");
	}

	public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
			throws IOException {
		MultivaluedMap<String, Object> headers = responseContext.getHeaders();
		headers.add("Access-Control-Allow-Origin", origin);
		headers.add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD");
		headers.add("Access-Control-Allow-Headers", "X-Requested-With, Content-Type, origin, accept, authorization");
		headers.add("Access-Control-Allow-Credentials","true");
	}
}
