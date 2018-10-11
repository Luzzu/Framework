package io.github.luzzu.communications.resources.v4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.communications.requests.RequestValidator;
import io.github.luzzu.communications.utils.APIExceptionJSONBuilder;
import io.github.luzzu.communications.utils.APIResponse;
import io.github.luzzu.communications.utils.CollectionToJSON;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.operations.ranking.ArbitraryRanking;
import io.github.luzzu.operations.ranking.RankBy;
import io.github.luzzu.operations.ranking.RankedObject;
import io.github.luzzu.operations.ranking.WeightedRanking;
import io.github.luzzu.operations.ranking.RankingConfiguration;

import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;

@Path("/v4/")
public class RankResource {
	final static Logger logger = LoggerFactory.getLogger(RankResource.class);
	private String JSON = MediaType.APPLICATION_JSON;

	@POST
	@Path("dataset/rank/weighted/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response rank(String message) {
		String jsonResponse = "";

		try {
			boolean validRequest = RequestValidator.weightedRankingValidator(message);
			if (validRequest) {
				logger.info("Weighed Ranking Requested: {}", message);
				ObjectMapper mapper = new ObjectMapper();
				JsonNode rootNode = mapper.readTree(message);
				List<RankingConfiguration> conf = new ArrayList<RankingConfiguration>();
				Iterator<JsonNode> iter = rootNode.elements();
				while(iter.hasNext()){
					JsonNode n = iter.next();
					Resource res = ModelFactory.createDefaultModel().createResource(n.get("uri").asText());
					
					RankBy type = n.get("type").asText().equals("category") ? RankBy.CATEGORY :
						n.get("type").asText().equals("dimension") ? RankBy.DIMENSION : RankBy.METRIC ;

					RankingConfiguration rc = new RankingConfiguration(res, type, n.get("weight").asDouble());
					conf.add(rc);
				}
				
				WeightedRanking rankObject = new WeightedRanking();
				List<RankedObject> ranking = rankObject.rank(conf, true);
				
				jsonResponse = CollectionToJSON.convert(ranking);
			}
		} catch (IllegalArgumentException | IOException e) {
			ExceptionOutput.output(e, "Failed Weighted Ranking",  logger);
			jsonResponse = new APIExceptionJSONBuilder("", e).toString();
		}
		return APIResponse.ok(jsonResponse.toString(), this.JSON);
	}
	
	@GET
	@Path("dataset/rank/arbitrary/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAllDatasets() {
		
		ArbitraryRanking rankObject = new ArbitraryRanking();
		List<RankedObject> ranking = rankObject.rank(null, true);
		
		String jsonResponse = CollectionToJSON.convert(ranking);
		
		return APIResponse.ok(jsonResponse.toString(), this.JSON);
	}
}
