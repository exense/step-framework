package step.framework.server;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;

import java.io.IOException;

public class CacheControlFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) {}

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (servletRequest instanceof Request && response instanceof HttpServletResponse) {
            Request request = (Request) servletRequest;
            HttpServletResponse httpResponse = (HttpServletResponse) response;

            // Check if the request is for the index HTML file which is also implicit for the base URL (i.e. path in context "/")
            // as it returns the welcome file (index.html) too
            if (request.getRequestURI().endsWith("index.html") || request.getPathInContext().equals("/")) {
                // Set Cache-Control header to never cache HTML files
                httpResponse.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
            }
        }
        // Continue with the next filter or servlet
        chain.doFilter(servletRequest, response);
    }

    @Override
    public void destroy() {}
}
