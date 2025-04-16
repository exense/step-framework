package step.framework.server.tables.service;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableRequestSortDeserializer extends JsonDeserializer {
    @Override
    public Object deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
        ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
        JsonNode node = mapper.readTree(jsonParser);
        List<Sort> result = new ArrayList<>();

        if (node.isArray()) {
            // Case: list of sort objects
            for (JsonNode itemNode : node) {
                Sort sortItem = mapper.treeToValue(itemNode, Sort.class);
                result.add(sortItem);
            }
        } else if (node.isObject()) {
            // Case: single sort object
            Sort singleSort = mapper.treeToValue(node, Sort.class);
            result.add(singleSort);
        } else if (!node.isNull()) {
            throw new JsonMappingException(jsonParser, "Expected object or array for 'sort' field");
        }

        return result;
    }
}
