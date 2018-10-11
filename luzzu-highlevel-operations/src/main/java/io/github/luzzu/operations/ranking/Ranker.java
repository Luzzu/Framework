package io.github.luzzu.operations.ranking;

import java.util.List;

public interface Ranker {

	List<RankedObject> rank(List<RankingConfiguration> rankingConfig);
	
	List<RankedObject> rank(List<RankingConfiguration> rankingConfig, boolean forceReloadCheck);
}
