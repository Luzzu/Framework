package io.github.luzzu.operations.ranking;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;


public class RankedObject implements Comparable<RankedObject>{
	
	final static Logger logger = LoggerFactory.getLogger(RankedObject.class);


	private String dataset;
	private double rankedValue;
	private String graphUri;
	
	public RankedObject(String dataset, double rankedValue, String graphURI){
		this.setDataset(dataset);
		this.setRankedValue(rankedValue);
		this.setGraphUri(graphURI);
	}
	
	@JsonProperty("Dataset-PLD")
	public String getDataset() {
		return dataset;
	}
	public void setDataset(String dataset) {
		this.dataset = dataset;
	}
	
	@JsonProperty("Rank-Value")
	public double getRankedValue() {
		return rankedValue;
	}
	public void setRankedValue(double rankedValue) {
		this.rankedValue = rankedValue;
	}
	
	@Override
	public int compareTo(RankedObject o) {
		if (this.rankedValue < o.getRankedValue()) return -1;
		if (this.rankedValue > o.getRankedValue()) return 1;
		else return 0;
	}
	
	@Override
	public boolean equals(Object other){
		if (other instanceof RankedObject){
			RankedObject _other = (RankedObject) other;
			return this.dataset.equals(_other.dataset);
		} else return false;
	}
	
	@Override
	public int hashCode(){
		return this.dataset.hashCode();
	}

	@JsonProperty("Graph-URI")
	public String getGraphUri() {
		return graphUri;
	}

	public void setGraphUri(String graphUri) {
		this.graphUri = graphUri;
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
	
}
