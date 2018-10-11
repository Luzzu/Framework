package io.github.luzzu.qualitymetrics.commons.cache;

import io.github.luzzu.operations.cache.CacheObject;

/**
 * @author Jeremy Debattista
 * 
 * This class creates a cached profile of the dataset being assessed
 */
public class CachedDatasetProfiling implements CacheObject {
	private static final long serialVersionUID = -3848853365217010700L;

	private String datasetURI = "";
	private String datasetLicense = "";
	private String sparqlEndPoint = "";
	private int datasetSize = -1;
	private boolean datasetSubset = false;
	
	public CachedDatasetProfiling(String datasetURI){
		this.datasetURI = datasetURI;
	}
	
	public String getDatasetLicense() {
		return datasetLicense;
	}
	public void setDatasetLicense(String datasetLicense) {
		this.datasetLicense = datasetLicense;
	}
	public String getDatasetURI() {
		return datasetURI;
	}
	public String getSparqlEndPoint() {
		return sparqlEndPoint;
	}

	public void setSparqlEndPoint(String sparqlEndPoint) {
		this.sparqlEndPoint = sparqlEndPoint;
	}

	public boolean isDatasetSubset() {
		return datasetSubset;
	}

	public void setDatasetSubset(boolean datasetSubset) {
		this.datasetSubset = datasetSubset;
	}

	public int getDatasetSize() {
		return datasetSize;
	}

	public void setDatasetSize(int datasetSize) {
		this.datasetSize = datasetSize;
	}
}
