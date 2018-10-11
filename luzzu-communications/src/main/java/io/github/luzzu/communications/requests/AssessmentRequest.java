package io.github.luzzu.communications.requests;

import java.text.ParseException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.communications.Main;
import io.github.luzzu.operations.lowlevel.Date;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;

public class AssessmentRequest {

	final static Logger logger = LoggerFactory.getLogger(AssessmentRequest.class);

	private String requestID;
	private String dataset;
	private AssessmentStatus status;
	private String agent;
	private String pld;
	private String date;
	private boolean isSparql;
	
	public AssessmentRequest(String dataset, String pld, boolean isSparql) {
		this.setRequestID(UUID.randomUUID().toString());
		this.setDataset(dataset);
		this.setPld(pld);		
		
		this.setDate((new Date()).getDate());
		
		this.setAgent(Main.BASE_URI);
		
		this.setStatus(AssessmentStatus.INPROGRESS);
		
		this.setSparql(isSparql);
	}

	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		String json = "{}";
		
		try {
			json = mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			ExceptionOutput.output(e, "API Exception object serialisation",  logger);
		}
		
		return json;
	}
	
	public boolean equals(Object obj) {
		if (obj == null) return false;
		if (!AssessmentRequest.class.isAssignableFrom(obj.getClass())) return false;
		
		final AssessmentRequest other = (AssessmentRequest) obj;
		return this.getRequestID().equals(other.getRequestID());
	}
	
	public int hashCode() {
		return this.getRequestID().hashCode();
	}
	
	@JsonProperty("Request-ID")
	public String getRequestID() {
		return requestID;
	}
	
	public void setRequestID(String requestID) {
		this.requestID = requestID;
	}
	
	@JsonProperty("Dataset-Location")
	public String getDataset() {
		return dataset;
	}
	public void setDataset(String dataset) {
		this.dataset = dataset;
	}
	
	@JsonProperty("Status")
	public AssessmentStatus getStatus() {
		return status;
	}
	public void setStatus(AssessmentStatus status) {
		this.status = status;
	}
	
	@JsonProperty("Agent")
	public String getAgent() {
		return agent;
	}
	public void setAgent(String agent) {
		this.agent = agent;
	}
	
	@JsonProperty("Dataset-PLD")
	public String getPld() {
		return pld;
	}
	public void setPld(String pld) {
		this.pld = pld;
	}

	@JsonProperty("Assessment-Date")
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public boolean isSparql() {
		return isSparql;
	}

	@JsonProperty("Is-Sparql-Endpoint")
	public void setSparql(boolean isSparql) {
		this.isSparql = isSparql;
	}
	
	public int compareTo(AssessmentRequest anotherObservation) {
		try {
			if (Date.stringToDate(this.getDate()).after(Date.stringToDate(anotherObservation.getDate()))) return 1;
			if (Date.stringToDate(this.getDate()).before(Date.stringToDate(anotherObservation.getDate()))) return -1;
		} catch (ParseException e) {
			return 0;
		}
		return 0;
	}
}