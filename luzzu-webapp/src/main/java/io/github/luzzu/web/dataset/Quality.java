package io.github.luzzu.web.dataset;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.operations.lowlevel.Date;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.operations.ranking.DatasetLoader;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.configuration.InternalModelConf;
import io.github.luzzu.semantics.datatypes.Observation;
import io.github.luzzu.semantics.utilities.ObservationHelper;
import io.github.luzzu.semantics.vocabularies.SDMXDIMENSION;
import io.github.luzzu.web.commons.StringUtils;
import io.github.luzzu.web.jsondatatypes.ComparingDatasetObject;
import io.github.luzzu.web.jsondatatypes.ComputedMetric;
import io.github.luzzu.web.jsondatatypes.ObservationObject;

public class Quality {
	final static Logger logger = LoggerFactory.getLogger(Quality.class);
	
	private static Dataset d = DatasetLoader.getInstance().getInternalDataset();
	private static Map<String, String> graphs = DatasetLoader.getInstance().getAllGraphs();
	
	public static Set<ComputedMetric> getLatestObservationForDataset(String datasetPLD){
		logger.info("[Quality Metadata] - Getting latest observations for all metrics in {}", datasetPLD);
		return getObservationForDataset(datasetPLD, null);
	}

	public static Set<String> getObservationDates(String datasetPLD) {
		logger.info("[Quality Metadata] - Getting Observation Dates for {}", datasetPLD);
		String graphName = graphs.get(StringUtils.strippedURI(datasetPLD));
		
		Model qualityMetadata = ModelFactory.createDefaultModel();
		qualityMetadata.add(d.getNamedModel(graphName));
		qualityMetadata.add(InternalModelConf.getFlatModel());
		
		Set<String> dates = new HashSet<String>();
		List<RDFNode> list = qualityMetadata.listObjectsOfProperty(SDMXDIMENSION.timePeriod).toList();
		
		list.forEach(res -> {
			String ldDate = res.asLiteral().getValue().toString();
			String jsonDate = null;
			try {
				jsonDate = StringUtils.toJSONDateFormat(ldDate);
			} catch (ParseException e) {
				ExceptionOutput.output(e, "[Quality Metadata] Cannot convert Linked Data date fromat to JSON date format for "+ ldDate, logger);
			}
			
			if (jsonDate != null) dates.add("\""+jsonDate+"\"");
		});
		
		return dates;
	}
	
	
	public static Set<ComputedMetric> getObservationForDataset(String datasetPLD, String date){
		return getObservationForDataset(null, datasetPLD, date);
	}
	
	public static Set<ComputedMetric> getObservationForDataset(Model m, String datasetPLD, String date){
		if (date != null) logger.info("[Quality Metadata] - Getting observations for all metrics in {} for date {}", datasetPLD, date);

		Model qualityMetadata = null;
		
		if (m == null) {
			String graphName = graphs.get(StringUtils.strippedURI(datasetPLD));
			
			qualityMetadata = ModelFactory.createDefaultModel();
			qualityMetadata.add(d.getNamedModel(graphName));
			qualityMetadata.add(InternalModelConf.getFlatModel());
		} else {
			qualityMetadata = m;
		}
	
		String query = "";
		try {
			query = StringUtils.getQueryFromFile("metrics/DatasetCDM.sparql");
		} catch (IOException e) {
			ExceptionOutput.output(e, "[Quality Metadata] Cannot retreive DatasetCDM.sparql for method getObservationForDataset(...)", logger);
		}
		
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(query), qualityMetadata);
		
		Set<ComputedMetric> mos = new HashSet<ComputedMetric>();
		ResultSet set = exec.execSelect();
		if (set.hasNext()) {
			set.forEachRemaining(sol -> {
				ComputedMetric mo = new ComputedMetric();
				
				String metricName = sol.get("metric_name").asLiteral().toString();
				String dimensionName = sol.get("dimension_name").asLiteral().toString();
				String categoryName = sol.get("category_name").asLiteral().toString();
				Resource metric = sol.get("metric").asResource();
				Resource metric_type = sol.get("metric_uri").asResource();
				
				mo.setInCategory(categoryName);
				mo.setInDimension(dimensionName);
				mo.setName(metricName);
				mo.setUri(metric.getURI());
				mo.setMetric_uri(metric_type);
				
				if (date == null) {
					Observation latestObservation = ObservationHelper.getLatestObservation(ObservationHelper.extractObservations(d.getNamedModel(graphName),metric_type));
					mo.addObservations(new ObservationObject(latestObservation));
				} else {
					List<Observation> obs = ObservationHelper.extractObservations(d.getNamedModel(graphName),metric_type);
					
					Optional<Observation> observation = obs.stream().filter(o -> Date.dateToString(o.getDateComputed()).equals(date)).findFirst();
					
					if (observation.isPresent()) 
						mo.addObservations(new ObservationObject(observation.get()));
				}
				
				if (mo.getObservations().size() > 0) mos.add(mo);
			});
		}
		return mos;
	}
	
	public static Set<ComparingDatasetObject> compareDatasetsOnMetric(Set<String> datasetPLDs, String metric) {
		Resource metricResource = ResourceCommons.toResource(metric);
		Set<ComparingDatasetObject> cdoSet = new HashSet<ComparingDatasetObject>();
		
		datasetPLDs.forEach(pld -> {
			String graphName = graphs.get(StringUtils.strippedURI(pld));
			Model qualityMetadata = d.getNamedModel(graphName);
			ObservationObject obs = new ObservationObject(ObservationHelper.getLatestObservation(ObservationHelper.extractObservations(qualityMetadata, metricResource)));
			ComparingDatasetObject datasetObject = new ComparingDatasetObject();
			datasetObject.setMetric(metric);
			datasetObject.setObservation(obs);
			datasetObject.setName(pld);
			cdoSet.add(datasetObject);
		});
		
		return cdoSet;
	}
}