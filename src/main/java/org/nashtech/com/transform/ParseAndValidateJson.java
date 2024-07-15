package org.nashtech.com.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.nashtech.com.exceptions.IncompleteJsonException;
import org.nashtech.com.exceptions.InvalidJsonSchemaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ParseAndValidateJson extends DoFn<String, Map<String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(ParseAndValidateJson.class);
    private final List<String> schema;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final StringBuilder jsonBuffer = new StringBuilder();

    public ParseAndValidateJson(List<String> schema) {
        this.schema = schema;
    }

    @ProcessElement
    public void processElement(@Element String json, OutputReceiver<Map<String, String>> out) {
        jsonBuffer.append(json);
        try {
            logger.info("Received JSON fragment: {}", json);
            JsonNode rootNode = objectMapper.readTree(jsonBuffer.toString());

            // Clear the buffer if the JSON is parsed successfully
            jsonBuffer.setLength(0);

            // Check if the root node is an array
            if (rootNode.isArray()) {
                Iterator<JsonNode> elements = rootNode.elements();
                while (elements.hasNext()) {
                    JsonNode elementNode = elements.next();
                    Map<String, String> map = convertToStringMap(elementNode);
                    validateAndOutput(map, out);
                }
            } else {
                Map<String, String> map = convertToStringMap(rootNode);
                validateAndOutput(map, out);
            }
        } catch (com.fasterxml.jackson.core.JsonParseException jsonParseException) {
            if (isIncompleteJsonException(jsonParseException)) {
                logger.info("JSON appears incomplete, buffering for next part: {}", jsonBuffer);
            } else {
                logger.error("Failed to parse JSON: {}", jsonBuffer, jsonParseException);
                throw new IncompleteJsonException("Failed to parse JSON: " + jsonBuffer);
            }
        } catch (IncompleteJsonException incompleteJsonException) {
            logger.error("Incomplete JSON: {}", jsonBuffer, incompleteJsonException);
            throw incompleteJsonException;
        } catch (InvalidJsonSchemaException invalidJsonSchemaException) {
            logger.error("Invalid JSON schema: {}", jsonBuffer, invalidJsonSchemaException);
            throw invalidJsonSchemaException;
        } catch (Exception unexpectedException) {
            logger.error("Unexpected error while parsing JSON: {}", jsonBuffer, unexpectedException);
            throw new RuntimeException("Unexpected error while parsing JSON: " + jsonBuffer, unexpectedException);
        }
    }

    private void validateAndOutput(Map<String, String> map, OutputReceiver<Map<String, String>> out) {
        if (!map.keySet().containsAll(schema)) {
            logger.error("Invalid JSON schema: expected {} but got {}", schema, map.keySet());
            throw new InvalidJsonSchemaException("Invalid JSON schema: " + map.keySet());
        }
        out.output(map);
    }

    private boolean isIncompleteJsonException(Exception jsonException) {
        return jsonException instanceof com.fasterxml.jackson.core.JsonParseException;
    }

    private Map<String, String> convertToStringMap(JsonNode node) {
        Map<String, String> map = new HashMap<>();
        node.fields().forEachRemaining(entry -> map.put(entry.getKey(), entry.getValue().asText()));
        return map;
    }
}
