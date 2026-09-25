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

import org.eclipse.jetty.server.AliasCheck;
import org.eclipse.jetty.util.URIUtil;
import org.eclipse.jetty.util.resource.Resource;

import java.net.URI;

/**
 * Approves the aliases whose real URI only differs from the resource URI by its spelling, i.e. both designate the
 * very same resource once normalized by Jetty.
 * <p>
 * This is the case of web resources served from a jar nested in a Spring Boot executable jar (such as the CLI) on
 * Unix-like systems: Jetty normalizes the resource URI to {@code jar:nested:///path/...}, while the nested file
 * system reports the real URI as {@code jar:nested:/path/...}. Without this check, Jetty treats every such resource as
 * an alias and rejects it, so the web application answers 404.
 * <p>
 * Actual aliases (symbolic links, case or 8.3 variants, ...) resolve to a different real URI and are left to the
 * other alias checks.
 */
public class UriSpellingAliasCheck implements AliasCheck {

    @Override
    public boolean checkAlias(String pathInContext, Resource resource) {
        // A combined resource iterates over its parts, any other resource over itself
        for (Resource part : resource) {
            if (part.isAlias() && !isSameResource(part.getURI(), part.getRealURI())) {
                return false;
            }
        }
        return true;
    }

    static boolean isSameResource(URI uri, URI realUri) {
        if (uri == null || realUri == null) {
            return false;
        }
        return normalize(uri).equals(normalize(realUri));
    }

    private static String normalize(URI uri) {
        String normalized = URIUtil.correctURI(uri).toASCIIString();
        // Directories are not consistently reported with a trailing slash
        return normalized.endsWith("/") ? normalized.substring(0, normalized.length() - 1) : normalized;
    }
}
