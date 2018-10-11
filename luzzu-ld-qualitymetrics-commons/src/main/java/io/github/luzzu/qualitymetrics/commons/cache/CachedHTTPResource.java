package io.github.luzzu.qualitymetrics.commons.cache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;

import io.github.luzzu.operations.cache.CacheObject;
import io.github.luzzu.qualitymetrics.commons.datatypes.HTTPDereference.StatusCode;
import io.github.luzzu.qualitymetrics.commons.serialisation.SerialisableHttpResponse;

/**
 * @author Jeremy Debattista
 * 
 */
public class CachedHTTPResource implements CacheObject {
	private static final long serialVersionUID = -5625345902018709236L;
	
	private String uri = "";
	private List<SerialisableHttpResponse> responses = null;
	private List<StatusLine> statusLines = null;
	private StatusCode dereferencabilityStatusCode = null;
	private Boolean parsableContent = null;
	private String content = null;
	
	public List<SerialisableHttpResponse> getResponses() {
		return responses;
	}
	public void addResponse(HttpResponse response) {
		if (this.responses == null) this.responses = new ArrayList<SerialisableHttpResponse>();
		
		this.responses.add(new SerialisableHttpResponse(response));
		this.addStatusLines(response.getStatusLine());
	}
	public void addAllResponses(List<HttpResponse> responses) {
		if (this.responses == null) this.responses = new ArrayList<SerialisableHttpResponse>();
		
		for(HttpResponse res : responses){
			this.responses.add(new SerialisableHttpResponse(res));
			this.addStatusLines(res.getStatusLine());
		}
	}
	public List<StatusLine> getStatusLines() {
		return statusLines;
	}
	public void addStatusLines(StatusLine statusLine) {
		if (this.statusLines == null) this.statusLines = new ArrayList<StatusLine>();
		synchronized(statusLines) {
			this.statusLines.add(statusLine);
		}
	}
	public void addAllStatusLines(List<StatusLine> statusLine) {
		if (this.statusLines == null) this.statusLines = new LinkedList<StatusLine>();
		synchronized(statusLines) {
			this.statusLines.addAll(statusLine);
		}
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public StatusCode getDereferencabilityStatusCode() {
		return dereferencabilityStatusCode;
	}
	public void setDereferencabilityStatusCode(StatusCode dereferencabilityStatusCode) {
		this.dereferencabilityStatusCode = dereferencabilityStatusCode;
	}
	
	public Boolean isContentParsable() {
		return parsableContent;
	}
	public void setParsableContent(boolean containsRDF) {
		this.parsableContent = containsRDF;
	}
	
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
}
