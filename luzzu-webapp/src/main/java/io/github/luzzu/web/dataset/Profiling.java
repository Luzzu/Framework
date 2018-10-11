package io.github.luzzu.web.dataset;

import java.io.IOException;
import java.util.Map;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.operations.ranking.DatasetLoader;
import io.github.luzzu.semantics.configuration.InternalModelConf;
import io.github.luzzu.web.commons.FieldType;
import io.github.luzzu.web.commons.StringUtils;
import io.github.luzzu.web.jsondatatypes.ObservationProfile;
import io.github.luzzu.web.jsondatatypes.observations.EstimationParameter;
import io.github.luzzu.web.jsondatatypes.observations.ProfilingProperty;
import io.github.luzzu.web.jsondatatypes.observations.ProvenanceProperty;


public class Profiling {

	final static Logger logger = LoggerFactory.getLogger(Profiling.class);
	
	private static Dataset d = DatasetLoader.getInstance().getInternalDataset();
	private static Map<String, String> graphs = DatasetLoader.getInstance().getAllGraphs();

	
	public static ObservationProfile getProfilingInformation(String observationID, String datasetPLD) {
		logger.info("[Dataset Profiling] - Getting Profiling Information for {} from {}", observationID, datasetPLD);
		String graphName = graphs.get(StringUtils.strippedURI(datasetPLD));
		
		Model qualityMetadata = ModelFactory.createDefaultModel();
		qualityMetadata.add(d.getNamedModel(graphName));
		qualityMetadata.add(InternalModelConf.getFlatModel());
		
		ObservationProfile profile  = new ObservationProfile(); // An observation can only have one profile

		// Check if the observation has profiling data
		String ask = "";
		try {
			ask = StringUtils.getQueryFromFile("profiling/HasProfilingData.sparql");
		} catch (IOException e) {
			ExceptionOutput.output(e, "[Dataset Profiling] Cannot retreive HasProfilingData.sparql for method getProfilingInformation(...)", logger);
		}
		
		ask = ask.replace("%metric-observation%", "<"+observationID+">");
		QueryExecution askQuery = QueryExecutionFactory.create(QueryFactory.create(ask), qualityMetadata);
		Boolean hasProfiling = askQuery.execAsk();
		
		if (hasProfiling) {
			String query = "";
			
			// Get basic profiling properties
			try {
				query = StringUtils.getQueryFromFile("profiling/GetObservationBasicProfilingData.sparql");
			} catch (IOException e) {
				ExceptionOutput.output(e, "[Dataset Profiling] Cannot retreive GetObservationBasicProfilingData.sparql for method getProfilingInformation(...)", logger);
			}
			
			query = query.replace("%metric-observation%", "<"+observationID+">");
			QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(query), qualityMetadata);
			ResultSet set = exec.execSelect();
			if (set.hasNext()) {
				set.forEachRemaining(sol -> {
					profile.setTotalDatasetTriples(sol.getLiteral("totalDatasetTriples") == null ? null : sol.getLiteral("totalDatasetTriples").getLong());
					profile.setTotalDatasetTriplesAssessed(sol.getLiteral("totalDatasetTriplesAssessed") == null ? null : sol.getLiteral("totalDatasetTriplesAssessed").getLong());
					RDFNode estimationTechnique = sol.get("estimationTechniqueUsed") == null ? null : sol.get("estimationTechniqueUsed");
					if (estimationTechnique != null) {
						profile.setEstimationTechniqueUsed(estimationTechnique.toString());
	
						if (estimationTechnique.isLiteral())
							profile.setEstimationTechniqueUsedType(FieldType.STRING);
						else 
							profile.setEstimationTechniqueUsedType(FieldType.URI);
					}
				});
			}
			
			// Get any estimation profiles
			if (profile.getEstimationTechniqueUsed() != null) {
				try {
					query = StringUtils.getQueryFromFile("profiling/GetObservationEstimationProfilingData.sparql");
				} catch (IOException e) {
					ExceptionOutput.output(e, "[Dataset Profiling] Cannot retreive GetObservationEstimationProfilingData.sparql for method getProfilingInformation(...)", logger);
				}
				
				query = query.replace("%metric-observation%", "<"+observationID+">");
				exec =  QueryExecutionFactory.create(QueryFactory.create(query), qualityMetadata);
				set = exec.execSelect();
				if (set.hasNext()) {
					set.forEachRemaining(sol -> {
						String epKey = sol.getLiteral("estimationParameterKey") == null ? null : sol.getLiteral("estimationParameterKey").getString();
						Double epValue = sol.getLiteral("estimationParameterValue") == null ? null : sol.getLiteral("estimationParameterValue").getDouble();
						EstimationParameter ep = new EstimationParameter(epKey,epValue);
						profile.addEstimationParameters(ep);
					});
				}
			}
			
			// Get any extra profiling properties
			try {
				query = StringUtils.getQueryFromFile("profiling/GetObservationExtraProfilingData.sparql");
			} catch (IOException e) {
				ExceptionOutput.output(e, "[Dataset Profiling] Cannot retreive GetObservationExtraProfilingData.sparql for method getProfilingInformation(...)", logger);
			}
			
			query = query.replace("%metric-observation%", "<"+observationID+">");
			exec =  QueryExecutionFactory.create(QueryFactory.create(query), qualityMetadata);
			set = exec.execSelect();
			if (set.hasNext()) {
				set.forEachRemaining(sol -> {
					ProfilingProperty pp = new ProfilingProperty(sol.getLiteral("propertyLabel").asLiteral().getString(), 
							sol.getLiteral("propertyComment").asLiteral().getString(), 
							sol.getLiteral("propertyValue").asLiteral().getValue());
					profile.addProfilingProperty(pp);
				});
			}
			
			// Get any extra provenance properties
			try {
				query = StringUtils.getQueryFromFile("profiling/GetObservationProvenanceProfilingData.sparql");
			} catch (IOException e) {
				ExceptionOutput.output(e, "[Dataset Profiling] Cannot retreive GetObservationExtraProfilingData.sparql for method getProfilingInformation(...)", logger);
			}
			
			query = query.replace("%metric-observation%", "<"+observationID+">");
			exec =  QueryExecutionFactory.create(QueryFactory.create(query), qualityMetadata);
			set = exec.execSelect();
			if (set.hasNext()) {
				set.forEachRemaining(sol -> {
					ProvenanceProperty pp = new ProvenanceProperty(sol.getLiteral("provPropertyLabel").asLiteral().getString(), 
							sol.getLiteral("provPropertyComment").asLiteral().getString(), 
							sol.getLiteral("provPropertyValue").asLiteral().getValue());
					profile.addProvenanceProperties(pp);
				});
			}
		}
		return profile;
	}
}
