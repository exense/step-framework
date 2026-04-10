package step.framework.server;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

public class CacheControlFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {

        // Cast to standard HttpServletRequest instead of Jetty's internal Request
        if (servletRequest instanceof HttpServletRequest && response instanceof HttpServletResponse) {
            HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
            HttpServletResponse httpResponse = (HttpServletResponse) response;

            // Calculate the path in context using standard Servlet methods
            String requestURI = httpRequest.getRequestURI();
            String contextPath = httpRequest.getContextPath();
            String pathInContext = requestURI.substring(contextPath.length());

            // Check if the request is for the index HTML file or the base URL "/"
            if (pathInContext.endsWith("index.html") || pathInContext.equals("/")) {
                // Set Cache-Control header to never cache HTML files
                httpResponse.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
            }
        }

        // Continue with the next filter or servlet
        chain.doFilter(servletRequest, response);
    }

    @Override
    public void destroy() {
    }
}
