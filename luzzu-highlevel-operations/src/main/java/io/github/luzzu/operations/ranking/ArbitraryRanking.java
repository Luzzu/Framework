package io.github.luzzu.operations.ranking;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArbitraryRanking implements Ranker {

	final static Logger logger = LoggerFactory.getLogger(ArbitraryRanking.class);
	
	private DatasetLoader dsLoader = DatasetLoader.getInstance();
	private Map<String,String> graphs = dsLoader.getAllGraphs();


	public List<RankedObject> rank(List<RankingConfiguration> rankingConfig){
		return rank(rankingConfig,false);
	}
	
	public List<RankedObject> rank(List<RankingConfiguration> rankingConfig, boolean forceReloadCheck){
		List<RankedObject> rankedObjects = new ArrayList<RankedObject>();
		
		if (forceReloadCheck){
			graphs = dsLoader.getAllGraphs();
		}
		
		for(String datasetPLD : graphs.keySet()){
			String metadataGraph = graphs.get(datasetPLD);
			
			RankedObject ro = new RankedObject(datasetPLD, 0.0, metadataGraph);
			rankedObjects.add(ro);
		}
		
		return rankedObjects;
	}
}
