package io.github.luzzu.web.ranking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.operations.ranking.DatasetLoader;
import io.github.luzzu.semantics.configuration.InternalModelConf;
import io.github.luzzu.web.commons.StringUtils;
import io.github.luzzu.web.jsondatatypes.CategoryMeasure;
import io.github.luzzu.web.jsondatatypes.DimensionMeasure;
import io.github.luzzu.web.jsondatatypes.MetricMeasure;


/**
 * @author Jeremy Debattista
 * 
 * This class handles all methods related to the 
 * loading of facet options such as the Category
 * Dimensions and Metrics
 *
 */
public class Facets {

	final static Logger logger = LoggerFactory.getLogger(Facets.class);
	private static Dataset metadata = DatasetLoader.getInstance().getInternalDataset();
	
	public static List<CategoryMeasure> getFacetOptions(){
		List<CategoryMeasure> categories = new ArrayList<CategoryMeasure>();
		
		metadata.addNamedModel("urn:InternalModelConfig", InternalModelConf.getFlatModel());
		
		String query = "";
		try {
			query = StringUtils.getQueryFromFile("operations/facets.sparql");
		} catch (IOException e) {
			ExceptionOutput.output(e, "[Facets] Cannot retreive facets.sparql for method getFacetOptions()", logger);
		}
		QueryExecution exec =  QueryExecutionFactory.create(QueryFactory.create(query), getFlatModel());

		ResultSet set = exec.execSelect();
		if (set.hasNext()) {
			set.forEachRemaining(qs -> {
				String catURI = qs.get("category").asResource().toString();
				String dimURI = qs.get("dimension").asResource().toString();
				String metURI = qs.get("metric").asResource().toString();
				
				String catLabel = qs.get("category_name").asLiteral().toString();
				String dimLabel = qs.get("dimension_name").asLiteral().toString();
				String metLabel = qs.get("metric_name").asLiteral().toString();
				
				String catComment = qs.get("category_comment").asLiteral().toString();
				String dimComment = qs.get("dimension_comment").asLiteral().toString();
				String metComment = qs.get("metric_comment").asLiteral().toString();
				
				CategoryMeasure cm = new CategoryMeasure();
				cm.setUri(catURI);
				int indexFound = -1;
				if (categories.contains(cm)) {
					cm = categories.get(categories.indexOf(cm));
					indexFound = categories.indexOf(cm);
				} else {
					cm.setLabel(catLabel);
					cm.setComment(catComment);
				}
				
				DimensionMeasure dm = new DimensionMeasure();
				dm.setUri(dimURI);
				if (cm.getDimensions().contains(dm)) {
					dm = cm.getDimensions().get(cm.getDimensions().indexOf(dm));
				} else {
					dm.setLabel(dimLabel);
					dm.setComment(dimComment);
					cm.addDimension(dm);
				}
				
				MetricMeasure mm = new MetricMeasure();
				mm.setUri(metURI);
				if (!(dm.getMetrics().contains(mm))) {
					mm.setLabel(metLabel);
					mm.setComment(metComment);
					dm.addMetric(mm);
				}
				
				if (indexFound != -1) categories.remove(indexFound);
				categories.add(cm);
			});
		}
		
		return categories;
	}
	
	private static Model getFlatModel() {
		Model m = ModelFactory.createDefaultModel();
		
		Iterator<String> iter = metadata.listNames();
		while (iter.hasNext()){
			m.add(metadata.getNamedModel(iter.next()));
		}
		m.add(metadata.getDefaultModel());
		
		return m;
	}
	
	
}