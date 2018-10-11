package io.github.luzzu.communications.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.communications.Main;
import io.github.luzzu.operations.lowlevel.Date;

public class APIExceptionJSONBuilder {
	
	final static Logger logger = LoggerFactory.getLogger(APIExceptionJSONBuilder.class);

	private String requestID;
	private Exception exception;
	private String date;
	private String agent;
	
	public APIExceptionJSONBuilder(String requestID, Exception exception) {
		this.setRequestID(requestID);
		this.setException(exception);
		
		this.setDate((new Date()).getDate());
		
		this.setAgent(Main.BASE_URI);
	}
	
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		String json = "{}";
		
		try {
			json = mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			System.out.println();
			System.out.printf("[Error - %s] API Exception object serialisation:", this.getDate());
			System.out.println(e.getMessage());
			logger.error("Error serialising API Exception as a JSON Object {}", e);
		}
		
		return json;
		
	}

	@JsonProperty("Request-ID")
	public String getRequestID() {
		return requestID;
	}

	public void setRequestID(String requestID) {
		this.requestID = requestID;
	}
	
	@JsonProperty("Exception")
	public String getException() {
		return exception.getMessage();
	}

	public void setException(Exception exception) {
		this.exception = exception;
	}

	@JsonProperty("Date")
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	@JsonProperty("Agent")
	public String getAgent() {
		return agent;
	}

	public void setAgent(String agent) {
		this.agent = agent;
	}

}
