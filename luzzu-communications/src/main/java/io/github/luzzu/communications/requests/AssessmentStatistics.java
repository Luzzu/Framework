package io.github.luzzu.communications.requests;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.communications.Main;
import io.github.luzzu.io.helper.IOStats;
import io.github.luzzu.operations.lowlevel.Date;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;

public class AssessmentStatistics {
	final static Logger logger = LoggerFactory.getLogger(AssessmentStatistics.class);

	private String requestID;
	private String agent;
	private String date;
	private Object[] stats;

	public AssessmentStatistics(String requestID, List<IOStats> stats) {
		this.setRequestID(requestID);
		this.setAgent(Main.BASE_URI);
		this.setDate((new Date()).getDate());
		this.setStats(stats.toArray());
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

	@JsonProperty("Request-ID")
	public String getRequestID() {
		return requestID;
	}

	public void setRequestID(String requestID) {
		this.requestID = requestID;
	}

	@JsonProperty("Agent")
	public String getAgent() {
		return agent;
	}

	public void setAgent(String agent) {
		this.agent = agent;
	}

	@JsonProperty("Date")
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	@JsonProperty("Statistics")
	public Object[] getStats() {
		return stats;
	}

	public void setStats(Object[] stats) {
		this.stats = stats;
	}
}
