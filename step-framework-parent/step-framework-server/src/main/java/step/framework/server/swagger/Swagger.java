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
package step.framework.server.swagger;

import io.swagger.v3.core.converter.ModelConverters;
import io.swagger.v3.jaxrs2.integration.resources.OpenApiResource;
import io.swagger.v3.oas.integration.SwaggerConfiguration;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import org.glassfish.jersey.server.ResourceConfig;
import step.core.AbstractContext;
import step.core.Version;

import java.util.List;

public class Swagger {

    public static void setup(ResourceConfig resourceConfig, AbstractContext context) {
        OpenAPI oas = new OpenAPI();
        Info info = new Info()
                .title("step Controller REST API")
                .description("")
                .version(context.require(Version.class).toString())
                .contact(new Contact()
                        .email("support@exense.ch"))
                .license(new License()
                        .name("GNU Affero General Public License")
                        .url("http://www.gnu.org/licenses/agpl-3.0.de.html"));

        oas.info(info);

        // SecurityScheme api-key
        SecurityScheme securitySchemeApiKey = new SecurityScheme();
        securitySchemeApiKey.setName("Api key");
        securitySchemeApiKey.setScheme("bearer");
        securitySchemeApiKey.setType(SecurityScheme.Type.HTTP);
        securitySchemeApiKey.setIn(SecurityScheme.In.HEADER);
        OpenApiResource openApiResource = new OpenApiResource();

        oas.schemaRequirement(securitySchemeApiKey.getName(), securitySchemeApiKey);

        Server server = new Server().url("/rest");
        oas.servers(List.of(server));
        oas.security(List.of(new SecurityRequirement().addList("Api key")));

        SwaggerConfiguration oasConfig = new SwaggerConfiguration()
                .openAPI(oas)
                .prettyPrint(true);

        openApiResource.setOpenApiConfiguration(oasConfig);
        resourceConfig.register(openApiResource);

        ModelConverters.getInstance().addConverter(new ObjectIdAwareConverter());
    }
}
