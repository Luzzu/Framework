package io.github.luzzu.qualitymetrics.commons.serialisation;

import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.sparql.graph.GraphFactory;

import io.github.luzzu.operations.cache.JenaCacheObject;


/**
 * @author Jeremy Debattista
 * 
 */
public class SerialisableModel extends ModelCom implements Serializable, JenaCacheObject<Model>{
	
	private static final long serialVersionUID = -6886059925250721814L;

	private transient Model model;
	private String _modelString = "";
	
	public SerialisableModel(){
		super(GraphFactory.createDefaultGraph());
	}

	public SerialisableModel(Model model){
		super(model.getGraph());
		this.model = model;
		
		StringWriter sw = new StringWriter();
		RDFDataMgr.write(sw, this.model, Lang.TURTLE);
		this._modelString = sw.toString();
	}
	
	public Model toJenaModel(){
		StringReader sr = new StringReader(this._modelString);
		RDFDataMgr.read(this.model, sr, "", Lang.TURTLE);
		return this.model;
	}

	@Override
	public Model deserialise() {
		return this.toJenaModel();
	}
}
