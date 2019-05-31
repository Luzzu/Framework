package io.github.luzzu.annotations;

import java.text.ParseException;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.DatasetImpl;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.assessment.QualityMetric;
import io.github.luzzu.exceptions.MetadataException;
import io.github.luzzu.operations.cache.CacheManager;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.operations.properties.PropertyManager;
import io.github.luzzu.qualitymetrics.commons.cache.TemporaryGraphMetadataCacheObject;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.utilities.DAQHelper;
import io.github.luzzu.semantics.vocabularies.CUBE;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.PROV;
import io.github.luzzu.semantics.vocabularies.SDMXDIMENSION;

/**
 * @author Jeremy Debattista
 * 
 * The Quality Metadata Class provides a number of methods
 * that enables the representation of Quality Metadata
 * as described by the Dataset Quality Ontology (DAQ).
 *
 */
public class QualityMetadata {

	private final CacheManager cacheMgr = CacheManager.getInstance();
	private final String cacheName = PropertyManager.getInstance().getProperties("luzzu.properties").getProperty("GRAPH_METADATA_CACHE");
	
	private Model metadata = ModelFactory.createDefaultModel();
	private Resource qualityGraph;
	boolean metadataPresent = false;
	private TemporaryGraphMetadataCacheObject _temp = null;
	private Resource computedOn;
	
	private Literal date = ResourceCommons.generateCurrentTime();

	final static Logger logger = LoggerFactory.getLogger(QualityMetadata.class);

	/**
	 * Since each assessed dataset should have only one quality 
	 * metadata graph, we need to check if it already exists
	 * in the cache.
	 * 
	 * @param datasetURI - The assessed dataset
	 * @param sparqlEndpoint - True if the datasetURI is a sparql endpoint.
	 */
	public QualityMetadata(Resource datasetURI, boolean sparqlEndpoint){
		this.computedOn = datasetURI;
		if (sparqlEndpoint){
			//TODO:sparqlendpoint
			//query, do not check in cache as it would not be feasible to store sparql endpoint results in cache
			//if exists set qualityGraphURI
		}
		
		if (cacheMgr.existsInCache(cacheName, datasetURI)){
			_temp = (TemporaryGraphMetadataCacheObject) cacheMgr.getFromCache(cacheName, datasetURI);
			this.qualityGraph = _temp.getGraphURI();
			this.metadata.add(_temp.getMetadataModel());
			this.metadataPresent = true;
		} else {
			this.qualityGraph = ResourceCommons.generateURI();
		}
	}
	
	/**
	 * Used when the assessed dataset is stored in memory 
	 * (Jena Dataset),
	 * 
	 * @param dataset - Assessed Jena Dataset
	 * @param computedOn - The resource indicating the metrics computed on
	 */
	public QualityMetadata(Dataset dataset, Resource computedOn){
		this.computedOn = computedOn;
		ResIterator qualityGraphRes = dataset.getDefaultModel().listSubjectsWithProperty(RDF.type, DAQ.QualityGraph);
		if (qualityGraphRes.hasNext()){
			this.qualityGraph = qualityGraphRes.next();
			this.metadata.add(dataset.getNamedModel(this.qualityGraph.getURI()));
			this.metadataPresent = true;
		} else {
			this.qualityGraph = ResourceCommons.generateURI();
		}
	}
	
	public QualityMetadata(Dataset dataset, Resource computedOn, String crawlDate){
		this(dataset,computedOn);
		String formatedDate = (new io.github.luzzu.operations.lowlevel.Date()).getRDFFormatDate();
		try {
			formatedDate = io.github.luzzu.operations.lowlevel.Date.getRDFFormatDate(crawlDate);
		} catch (ParseException e) {
			ExceptionOutput.output(e, "Error in Parsing Crawl Date in Quality Metadata generation.", logger);
		}
		this.date = ModelFactory.createDefaultModel().createTypedLiteral(formatedDate, XSDDatatype.XSDdate);
	}
	
	public QualityMetadata(Resource computedOn, boolean sparqlEndpoint, String crawlDate){
		this(computedOn,sparqlEndpoint);
		String formatedDate = (new io.github.luzzu.operations.lowlevel.Date()).getRDFFormatDate();
		try {
			formatedDate = io.github.luzzu.operations.lowlevel.Date.getRDFFormatDate(crawlDate);
		} catch (ParseException e) {
			ExceptionOutput.output(e, "Error in Parsing Crawl Date in Quality Metadata generation.", logger);
		}
		this.date = ModelFactory.createDefaultModel().createTypedLiteral(formatedDate, XSDDatatype.XSDdate);
	}
	
	

	/**
	 * Creates observation data for the assessed metric
	 * 
	 * @param metric - Metric Class
	 * 
	 * @return Observation URI which will be used for the quality problem report
	 */
	public Resource addMetricData(QualityMetric<?> metric) throws MetadataException {
		Resource categoryURI = null;
		try {
			Resource categoryType = DAQHelper.getCategoryResource(metric.getMetricURI());
			categoryURI = this.categoryExists(categoryType);		
			if (categoryURI == null){
				categoryURI = ResourceCommons.generateURI();
				this.metadata.add(categoryURI, RDF.type, categoryType);
			}
		} catch (Exception e) {
			throw new MetadataException("Error in retreiving category for metric "+metric.getMetricURI()+ " from schema.");
		}
		
		Resource dimensionURI = null;
		try {
			Resource dimensionType = DAQHelper.getDimensionResource(metric.getMetricURI());
			dimensionURI = this.dimensionExists(dimensionType);
			if (dimensionURI == null){
				dimensionURI = ResourceCommons.generateURI();
				Property dimensionProperty = this.metadata.createProperty(DAQHelper.getPropertyResource(dimensionType).getURI());
				this.metadata.add(categoryURI, dimensionProperty, dimensionURI);
				this.metadata.add(dimensionURI, RDF.type, dimensionType);
			}
		} catch (Exception e) {
			throw new MetadataException("Error in retreiving dimension for metric "+metric.getMetricURI()+ " from schema.");
		}
		
		Resource metricURI = null;
		try {
		Resource metricType = metric.getMetricURI();
		metricURI = this.metricExists(metricType);
			if (metricURI == null){
				metricURI = ResourceCommons.generateURI();
				Property metricProperty = this.metadata.createProperty(DAQHelper.getPropertyResource(metricType).getURI());
				this.metadata.add(dimensionURI, metricProperty, metricURI);
				this.metadata.add(metricURI, RDF.type, metricType);
			}
		} catch (Exception e) {
			throw new MetadataException("Error in retreiving metric from schema: "+metric.getMetricURI());
		}
		
		Resource observationURI = ResourceCommons.generateURI();
		this.metadata.add(metricURI, DAQ.hasObservation, observationURI);
		
		this.metadata.add(observationURI, RDF.type, DAQ.Observation);
		this.metadata.add(observationURI, SDMXDIMENSION.timePeriod, date);
		this.metadata.add(observationURI, DAQ.metric, metricURI);
		this.metadata.add(observationURI, DAQ.computedOn, this.computedOn);
		this.metadata.add(observationURI, DAQ.value, ResourceCommons.generateTypeLiteral(metric.metricValue()));
		this.metadata.add(observationURI, DAQ.isEstimate, ResourceCommons.generateBooleanTypeLiteral(metric.isEstimate()));

		
		if (metric.getAgentURI() != null) {
			this.metadata.add(observationURI, PROV.wasGeneratedBy, metric.getAgentURI());
		}
		
		Model obsActivity = metric.getObservationActivity();
		if ((obsActivity != null) && (obsActivity.size() > 0)) {
			ResIterator itr = obsActivity.listSubjectsWithProperty(RDF.type, ResourceCommons.asRDFNode(DAQ.MetricProfile.asNode()));
			Resource subj = itr.nextResource();
			
			this.metadata.add(observationURI, PROV.generated, subj);
			this.metadata.add(obsActivity);
		}
		
		this.metadata.add(observationURI, CUBE.dataSet, qualityGraph);
		
		return observationURI;
	}
	
	/**
	 * Creates quality metadata
	 * 
	 * @return Dataset with quality metadata which needs to be attached to the assessed dataset.
	 * @throws MetadataException if there is no observation data calculated.
	 */
	public Dataset createQualityMetadata() throws MetadataException{
		Model defaultModel = ModelFactory.createDefaultModel();
		Dataset dataset = null;
		
		if (this.metadata.size() == 0) throw new MetadataException("No Metric Observations Recorded");
		
		defaultModel.add(qualityGraph, RDF.type, DAQ.QualityGraph);
		defaultModel.add(qualityGraph, CUBE.structure, DAQ.dsd);
		dataset = new DatasetImpl(defaultModel);
		dataset.addNamedModel(this.qualityGraph.getURI(), this.metadata);

		return dataset;
	}
	
	/**
	 * Checks if a category uri exists in the metadata
	 * 
	 * @param categoryType - The URI of the Category Type
	 * @return The URI if exists or null
	 */
	private Resource categoryExists(Resource categoryType){
		ResIterator resIte = this.metadata.listSubjectsWithProperty(RDF.type, categoryType);
		if (resIte.hasNext()){
			return resIte.next();
		}
		return null;
	}
	
	/**
	 * Checks if a dimension uri exists in the metadata
	 * 
	 * @param dimensionType - The URI of the Dimension Type
	 * @return The URI if exists or null
	 */
	private Resource dimensionExists(Resource dimensionType){
		ResIterator resIte = this.metadata.listSubjectsWithProperty(RDF.type, dimensionType);
		if (resIte.hasNext()){
			return resIte.next();
		}
		return null;
	}
	
	/**
	 * Checks if a metric uri exists in the metadata
	 * 
	 * @param metricType - The URI of the Metric Type
	 * @return The URI if exists or null
	 */
	private Resource metricExists(Resource metricType){
		ResIterator resIte = this.metadata.listSubjectsWithProperty(RDF.type, metricType);
		if (resIte.hasNext()){
			return resIte.next();
		}
		return null;
	}
}
