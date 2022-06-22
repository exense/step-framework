package step.framework.server;


import com.fasterxml.jackson.databind.ObjectMapper;
import step.core.accessors.DefaultJacksonMapperProvider;

import jakarta.ws.rs.ext.ContextResolver;

public class JacksonMapperProvider implements ContextResolver<ObjectMapper> {

    private final ObjectMapper mapper;

    public JacksonMapperProvider() {
        mapper = DefaultJacksonMapperProvider.getObjectMapper();
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }

}
