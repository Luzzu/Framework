package io.github.luzzu.qualitymetrics.commons.serialisation;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpResponse;

public class SerialisableHttpResponse implements Serializable{
	private static final long serialVersionUID = 5007740429193218086L;
	private Map<String,String> headers = new HashMap<String,String>();
	
	public SerialisableHttpResponse(HttpResponse _response){
		for(Header h : _response.getAllHeaders()) headers.put(h.getName(), h.getValue());
	}

	public String getHeaders(String name){
		return headers.get(name);
	}
}