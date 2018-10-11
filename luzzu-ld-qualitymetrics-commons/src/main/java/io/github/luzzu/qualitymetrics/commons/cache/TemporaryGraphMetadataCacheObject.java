package io.github.luzzu.qualitymetrics.commons.cache;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;

import io.github.luzzu.operations.cache.CacheObject;
import io.github.luzzu.semantics.commons.ResourceCommons;

/**
 * @author Jeremy Debattista
 * 
 * Datastructure for caching Quality Metadata Graphs
 *  
 */
public class TemporaryGraphMetadataCacheObject implements CacheObject {

	/**
	 * Serial Version
	 */
	private static final long serialVersionUID = -286271367636630291L;
	
	transient private Resource graphURI = null;
	transient private Model metadataModel = null;
	
	public TemporaryGraphMetadataCacheObject(Resource graphURI){
		this.graphURI = graphURI;
		this.metadataModel = ModelFactory.createDefaultModel();
	}
	
	public void addTriplesToMetadata(Quad quad){
		Property p = metadataModel.createProperty(quad.getPredicate().getURI());
		metadataModel.add(ResourceCommons.asRDFNode(quad.getSubject()).asResource(), p, ResourceCommons.asRDFNode(quad.getObject()));
	}
	
	public void addModelToMetadata(Model model){
		metadataModel.add(model);
	}
	
	public Resource getGraphURI(){
		return this.graphURI;
	}
	
	public Model getMetadataModel(){
		return this.metadataModel;
	}
}
