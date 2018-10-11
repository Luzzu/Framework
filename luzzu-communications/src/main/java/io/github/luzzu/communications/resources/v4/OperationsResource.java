package io.github.luzzu.communications.resources.v4;

import java.io.StringWriter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.communications.utils.CollectionToJSON;
import io.github.luzzu.web.assessment.MetricConfiguration;
import io.github.luzzu.web.ranking.Facets;

@Path("/v4/")
public class OperationsResource {

	final static Logger logger = LoggerFactory.getLogger(OperationsResource.class);
	
	@GET
	@Path("framework/available-metrics/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLoadedMetrics(){
		Model m = MetricConfiguration.getAllMetrics();
				
		StringWriter strWriter = new StringWriter();
		RDFDataMgr.write(strWriter, m, RDFFormat.JSONLD);
		
		return Response.ok(strWriter.toString(),MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
			      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
	}
	
	@GET
	@Path("framework/filtering-facets/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getRankingFacetOptions(){
		return Response.ok(CollectionToJSON.convert(Facets.getFacetOptions(), "Categories"),MediaType.APPLICATION_JSON).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
			      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
	}
}
