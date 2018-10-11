package io.github.luzzu.communications.utils;

import java.util.Collection;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public class CollectionToJSON {

	public static String convert(Collection<?> collection, String fieldName) {
		ObjectMapper mapper = new ObjectMapper();
		
		ObjectNode obj = mapper.createObjectNode();
		ArrayNode array = obj.putArray(fieldName);
		
		collection.forEach(item -> {
			array.addPOJO(item);
		});
		
		return obj.toString();
	}
	
	public static String convert(Collection<?> collection) {
		ObjectMapper mapper = new ObjectMapper();
		
		ArrayNode array = mapper.createArrayNode();
		
		collection.forEach(item -> {
			array.addPOJO(item);
		});
		
		return array.toString();
	}
}
