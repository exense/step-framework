package step.framework.server.tables;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Assert;
import org.junit.Test;
import step.core.accessors.DefaultJacksonMapperProvider;
import step.framework.server.tables.service.TableRequest;

public class TableRequestTest {

    private static final ObjectMapper om = DefaultJacksonMapperProvider.getObjectMapper();

    @Test
    public void testInternalNotExposedInJson() throws Exception {
        TableRequest tr = new TableRequest();
        Assert.assertFalse(om.writeValueAsString(tr).contains("internalRequest"));
    }

    @Test
    public void testInternalNotAcceptedInJson() throws Exception {
        String wrongInput = "{\"filters\":null,\"tableParameters\":null,\"skip\":null,\"limit\":null,\"sort\":null,\"performEnrichment\":true,\"calculateCounts\":true,\"internalRequest\":true}";
        Assert.assertFalse(om.readValue(wrongInput, TableRequest.class).isInternalRequest());
    }

}
