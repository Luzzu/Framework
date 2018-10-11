package io.github.luzzu.operations.ranking;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.AtomicDouble;

import io.github.luzzu.semantics.configuration.InternalModelConf;
import io.github.luzzu.semantics.datatypes.Observation;
import io.github.luzzu.semantics.utilities.ObservationHelper;
import io.github.luzzu.semantics.utilities.SPARQLHelper;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.LQM;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.XSD;

public class WeightedRanking implements Ranker {

	final static Logger logger = LoggerFactory.getLogger(WeightedRanking.class);
	
	private DatasetLoader dsLoader = DatasetLoader.getInstance();
	private Dataset d = dsLoader.getInternalDataset();
	private Map<String,String> graphs = dsLoader.getAllGraphs();

	private static Model luzzuInternalModel = InternalModelConf.getFlatModel(); // This retrieves the internal model which contains definitions of the metrics etc...
	
	private int isNaNMetrics = 0;
	private int isNaNDimension = 0;
	
	
	// Relative Positioning of non double values
	private Map<Resource, List<RankedObject>> metricRelativePositions = new HashMap<Resource,List<RankedObject>>();

	public List<RankedObject> rank(List<RankingConfiguration> rankingConfig){
		return rank(rankingConfig,false);
	}
	
	public List<RankedObject> rank(List<RankingConfiguration> rankingConfig, boolean forceReloadCheck){
		this.integerToPosition();
		this.dateToPosition();
		
		List<RankedObject> rankedObjects = new ArrayList<RankedObject>();
		
		if (forceReloadCheck){
			graphs = dsLoader.getAllGraphs();
		}
		
		for(String datasetPLD : graphs.keySet()){
			String metadataGraph = graphs.get(datasetPLD);
			Model metadataModel = d.getNamedModel(metadataGraph);
			Double rankedValue = 0.0;
			
			for(RankingConfiguration rc : rankingConfig){
				if (rc.getType() == RankBy.CATEGORY){
					rankedValue += this.categoryValue(rc.getUriResource(), rc.getWeight(), metadataModel, datasetPLD);
				}
				if (rc.getType() == RankBy.DIMENSION){
					rankedValue += this.dimensionValue(rc.getUriResource(), rc.getWeight(), metadataModel, datasetPLD);
				}
				if (rc.getType() == RankBy.METRIC){
					rankedValue += this.metricValue(rc.getUriResource(), rc.getWeight(), metadataModel, datasetPLD);
				}
			}
			
			if (!(rankedValue.isNaN())){
				RankedObject ro = new RankedObject(datasetPLD, rankedValue, metadataGraph);
				rankedObjects.add(ro);
			}
		}
		Collections.sort(rankedObjects);
		Collections.reverse(rankedObjects);
		return rankedObjects;
	}
	
	
	private double metricValue(Resource metric, double weight, Model qualityMetadata, String datasetPLD){
		Double weightedValue = Double.NaN;
		logger.info("Ranking by Metric {}, with the weight of {}",metric.getURI(), weight);

		Optional<RDFNode> optionalDataType = luzzuInternalModel.listObjectsOfProperty(metric, DAQ.expectedDataType).nextOptional();
		RDFNode metricExpectedDataType = null;
		if (optionalDataType.isPresent()) metricExpectedDataType = optionalDataType.get();

		if (metricExpectedDataType != null) {
			List<Observation> lst = ObservationHelper.extractObservations(qualityMetadata, metric);
			if (lst.size() > 0){
				Observation obs = ObservationHelper.getLatestObservation(lst);
				if (metricExpectedDataType.asResource().equals(XSD.xdouble)) {
					Double value = (Double) obs.getValue();
					weightedValue = value * weight;
				} else if (metricExpectedDataType.asResource().equals(XSD.integer)) {
					weightedValue = (getRelativePercentage(metric, datasetPLD) / 100.0) * weight;
				} else if (metricExpectedDataType.asResource().equals(XSD.xlong)) {
					weightedValue = (getRelativePercentage(metric, datasetPLD) / 100.0) * weight;
				} else if (metricExpectedDataType.asResource().equals(XSD.xboolean)) {
					Boolean value = (Boolean) obs.getValue();
					if (metric.equals(LQM.SyntaxErrorsMetric))
						weightedValue = ((value) ? 0.0 : 1.0) * weight;
					else
						weightedValue = ((value) ? 1.0 : 0.0) * weight;
				} else if (metricExpectedDataType.asResource().equals(XSD.date) || metricExpectedDataType.asResource().equals(XSD.dateTime)) {
					weightedValue = getRelativePercentage(metric, datasetPLD) * weight;
				} else {
					logger.debug("Cannot calculate weight for {} datatype", obs.getValue().getClass().getSimpleName());
				}
			}
		}
		return weightedValue;
	}
	
	
	
	private double dimensionValue(Resource dimension, double weight, Model qualityMetadata, String datasetPLD){
		logger.info("Dimension: {}; Weight: {}, Dataset: {}",dimension.getURI(), weight, datasetPLD);
		String selectQuery = this.getSelectQuery("sparql/GetDimensionMetrics.sparql").replace("%dimension%", SPARQLHelper.toSPARQL(dimension));
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), InternalModelConf.getFlatModel());
		ResultSet set = exec.execSelect();
		
		this.isNaNMetrics = 0;
		AtomicDouble summation = new AtomicDouble();
		AtomicDouble totalNumberOfMetrics =  new AtomicDouble();
		
		if (set.hasNext()) {
			set.forEachRemaining(qs -> {
				totalNumberOfMetrics.addAndGet(1.0);
				Resource metric = qs.get("metric").asResource();
				Double metricValue = this.metricValue(metric, weight, qualityMetadata, datasetPLD);
				if (!(metricValue.isNaN())) summation.addAndGet(metricValue);
				else this.isNaNMetrics++;
			});
		}
		
		if ((totalNumberOfMetrics.get() == 0.0) || (totalNumberOfMetrics.get() == this.isNaNMetrics)){
			logger.info("No metrics available for the {} dimension", dimension.getURI());
			this.isNaNDimension++;
			return Double.NaN;
		} else {
			double dimensionRanking = summation.get() / totalNumberOfMetrics.get();
			logger.info("Ranking value for {} computed: {}",dimension.getURI(),dimensionRanking);
			
			return dimensionRanking;
		}
	}
	
	private double categoryValue(Resource category, double weight, Model qualityMetadata, String datasetPLD){
		logger.info("Category: {}; Weight: {}, Dataset: {}",category.getURI(), weight, datasetPLD);
		String selectQuery = this.getSelectQuery("sparql/GetCategoryDimensions.sparql").replace("%category%", SPARQLHelper.toSPARQL(category));
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(selectQuery), InternalModelConf.getFlatModel());
		ResultSet set = exec.execSelect();
		
		
		this.isNaNDimension = 0;
		AtomicDouble summation = new AtomicDouble();
		AtomicDouble totalNumberOfDimensions =  new AtomicDouble();
		
		if (set.hasNext()) {
			set.forEachRemaining(qs -> {
				totalNumberOfDimensions.addAndGet(1.0);
				Resource dimension = qs.get("dimension").asResource();
				Double dimensionValue = this.dimensionValue(dimension, weight, qualityMetadata, datasetPLD);
				if (!(dimensionValue.isNaN())) summation.addAndGet(dimensionValue);
				else this.isNaNDimension++;
			});
		}

		if ((totalNumberOfDimensions.get() == 0.0) || (totalNumberOfDimensions.get() == this.isNaNDimension)){
			logger.info("No dimensions available for the {} Category",category.getURI());
			return Double.NaN;
		} else {
			double categoryRanking = summation.get() / totalNumberOfDimensions.get();
			logger.info("Ranking value for {} computed: {}",category.getURI(),categoryRanking);
			return categoryRanking;
		}
	}
		
	private String getSelectQuery(String fileName){
		String selectQuery = "";
		URL url = Resources.getResource(fileName);
		try {
			selectQuery = Resources.toString(url, Charsets.UTF_8);
		} catch (IOException e) {
			logger.error("Error: {}",e.getMessage());
		}
		
		return selectQuery;
	}

	
	private void integerToPosition() {
		this.relativePositioning(Integer.class);
	}
	
	private void dateToPosition() {
		this.relativePositioning(XSDDateTime.class);
	}

	private void relativePositioning(Class<?> clazz) {
		Set<Resource> metricsSet = new HashSet<Resource>();
		
		if (clazz == Integer.class) {
			metricsSet.addAll(luzzuInternalModel.listResourcesWithProperty(DAQ.expectedDataType, XSD.integer.asNode()).toSet());
			metricsSet.addAll(luzzuInternalModel.listResourcesWithProperty(DAQ.expectedDataType, XSD.xlong.asNode()).toSet());
		}
		

		if (clazz == XSDDateTime.class) {
			metricsSet.addAll(luzzuInternalModel.listResourcesWithProperty(DAQ.expectedDataType, XSD.dateTime.asNode()).toSet());
			metricsSet.addAll(luzzuInternalModel.listResourcesWithProperty(DAQ.expectedDataType, XSD.date.asNode()).toSet());
		}
		
		if (metricsSet.size() > 0) {
			metricsSet.forEach(metric -> {
				List<RankedObject> ros = new ArrayList<RankedObject>();
				for(String pld : graphs.keySet()){
					String graphURI = graphs.get(pld);
					Model metadata = d.getNamedModel(graphURI);
					
					List<Observation> lst = ObservationHelper.extractObservations(metadata, metric);
					if (lst.size() > 0) {
						Observation obs = ObservationHelper.getLatestObservation(lst);
						
						if (clazz == Integer.class) {
							RankedObject ro = new RankedObject(pld, (Integer) obs.getValue(), graphURI);
							ros.add(ro);
						}
						
						if (clazz == XSDDateTime.class) {
							XSDDateTime dateTime = (XSDDateTime) obs.getValue();
							Long time = dateTime.asCalendar().getTime().getTime();
							RankedObject ro = new RankedObject(pld, (Long) time, graphURI);
							ros.add(ro);
						}
					}
				}
				Collections.sort(ros);
				Collections.reverse(ros);
				metricRelativePositions.put(metric, ros);
			});
		}
	}
	
	
	private double getRelativePercentage(Resource metric, String datasetPLD) {
		if (metricRelativePositions.containsKey(metric)) {
			List<RankedObject> ranks = metricRelativePositions.get(metric);
			List<RelativePosition> relativeRanks = this.setRelativePosition(ranks);
			
			RelativePosition tmp = new RelativePosition(datasetPLD,-1);
			int bottomPos = relativeRanks.get(relativeRanks.size() - 1).getPos();
			double val = 0.0d;
			
			
			if (relativeRanks.get(relativeRanks.indexOf(tmp)).getPos() == 1) val = 100.0d;
			else if (relativeRanks.get(relativeRanks.indexOf(tmp)).getPos() == bottomPos) val = 0.0d;
			else {
				int thePos = relativeRanks.get(relativeRanks.indexOf(tmp)).getPos();
				val = ((((double)ranks.size() + 1) - (double)thePos)) / (double) ranks.size(); 
				// multiply the denominator by 100 to get % value
				// ranks.size() + 1 because lists starts from 0 and the first item position is set to 1
			}
			return val;
		} else
			return Double.NaN;
	}
	
	private List<RelativePosition> setRelativePosition(List<RankedObject> obj){
		List<RelativePosition> lst = new ArrayList<RelativePosition>();
		
		int rank = 0;
		double lastValue = -1.0;
		
		for(RankedObject o : obj){
			double val = o.getRankedValue();
			if (lastValue != val){
				rank = obj.indexOf(o) + 1;
				lastValue = o.getRankedValue();
			} 
			RelativePosition _rp = new RelativePosition(o.getDataset(), rank);
			lst.add(_rp);
		}
		
		return lst;
	}
	
	private class RelativePosition{
		private int pos = 0;
		private String datasetPLD = "";
		
		public RelativePosition(String datasetPLD, int pos){
			this.pos = pos;
			this.datasetPLD = datasetPLD;
		}
		
		public int getPos(){
			return pos;
		}
		
		@Override
		public boolean equals(Object other){
			if (other instanceof RelativePosition){
				RelativePosition _other = (RelativePosition) other;
				return this.datasetPLD.equals(_other.datasetPLD);
			} else return false;
		}
		
		@Override
		public int hashCode(){
			return this.datasetPLD.hashCode();
		}
	}

}
