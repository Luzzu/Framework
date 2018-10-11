package io.github.luzzu.io.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jena.riot.RiotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;

import io.github.luzzu.datatypes.Object2Quad;
import io.github.luzzu.exceptions.EndpointException;
import io.github.luzzu.exceptions.ExternalMetricLoaderException;
import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.exceptions.ProcessorNotInitialised;
import io.github.luzzu.io.AbstractIOProcessor;
import io.github.luzzu.io.helper.MetricProcess;
import io.github.luzzu.io.helper.StreamMetadataSniffer;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;


public class SPARQLEndPointProcessor extends AbstractIOProcessor {
	
	final static Logger logger = LoggerFactory.getLogger(SPARQLEndPointProcessor.class);

	private ConcurrentLinkedQueue<QuerySolution> sparqlIterator = new ConcurrentLinkedQueue<QuerySolution>();
	
	
	public SPARQLEndPointProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration){
		super(datasetPLD, datasetLocation, genQualityReport, configuration);
		super.logger = SPARQLEndPointProcessor.logger;
	}
	
	public SPARQLEndPointProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration, String crawlDate){
		super(datasetPLD, datasetLocation, genQualityReport, configuration, crawlDate);
		super.logger = SPARQLEndPointProcessor.logger;
	}
	
	
	@Override
	public void setUpProcess() throws LuzzuIOException {
		logger.debug("Setting up SPARQL Processor");
		
		try {
			loadMetrics();
			this.executor = Executors.newSingleThreadExecutor();
			this.isInitalised = true;
		} catch (ExternalMetricLoaderException e) {
			ExceptionOutput.output(e, "Error parsing dataset: "+ datasetLocation, logger);
		}	
	}

	@Override
	public void startProcessing() throws LuzzuIOException {
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	
		StreamMetadataSniffer sniffer = new StreamMetadataSniffer();
		
		int size = -1;
		executor = Executors.newSingleThreadExecutor();

		final String sparqlEndPoint = this.datasetLocation;
		
		try{
			final Future<Integer> handler = executor.submit(new Callable<Integer>() {
			    @Override
			    public Integer call() throws Exception{ 
					String query = "SELECT DISTINCT (count(?s) AS ?count) { { ?s ?p ?o . } UNION { GRAPH ?g { ?s ?p ?o .} } }";
					QueryEngineHTTP qe = (QueryEngineHTTP) QueryExecutionFactory.sparqlService(sparqlEndPoint,query);

					int size = qe.execSelect().next().get("count").asLiteral().getInt();
			    		return size;
			    }
			});
			
			try {
				size = handler.get(1, TimeUnit.MINUTES);
			} catch (TimeoutException e) {
				handler.cancel(true);
				EndpointException epe =  new EndpointException("Endpoint Timeout Exception for: "+ sparqlEndPoint + " " + e.getMessage());
				ExceptionOutput.output(epe, "Timeout in SPARQL Endpoint: "+ datasetLocation, logger);
				throw epe;
			}
		} catch (Exception e){
			EndpointException epe =  new EndpointException("Endpoint Exception for: "+ sparqlEndPoint + " " + e.getMessage());
			ExceptionOutput.output(epe, "Exception thrown on SPARQL Endpoint: "+ datasetLocation, logger);
			throw epe;
		}
			
		final int endpointSize = size;
		logger.info("number of triples {}", endpointSize);
		
		Future<?> _futureParser = null;
		if (size > -1){
			Runnable parser = new Runnable(){
				int nextOffset = 0;
				public void run(){
					try{
						boolean start = true;
						do{
							if (nextOffset >= endpointSize) 
								start = false;
							
							logger.debug("[SPARQL Endpoint Processor - {}] Endpoint: {} => Next offset {}, Size {}",
									(new io.github.luzzu.operations.lowlevel.Date()).getDate(), sparqlEndPoint, nextOffset, endpointSize);
							
							String query = "SELECT * WHERE { { SELECT DISTINCT * { { ?s ?p ?o . } UNION { GRAPH ?g { ?s ?p ?o .} } } ORDER BY ASC(?s) } } LIMIT 10000 OFFSET " + nextOffset;
							
							QueryEngineHTTP qe = (QueryEngineHTTP) QueryExecutionFactory.sparqlService(sparqlEndPoint, query);
							qe.addParam("timeout","10000"); 
							ResultSet rs = qe.execSelect();
							while(rs.hasNext()){
								sparqlIterator.add(rs.next());
							}
							
							nextOffset = ((endpointSize - nextOffset) > 10000) ? nextOffset + 10000 : nextOffset + (endpointSize - nextOffset);
						} while(start);
						logger.info("[SPARQL Endpoint Processor - {}] - Done Parsing Endpoint {}",
								(new io.github.luzzu.operations.lowlevel.Date()).getDate(), sparqlEndPoint);
					} catch (Exception e){
						logger.error("[SPARQL Endpoint Processor - {}] - Error parsing SPARQL Endpoint {}. Error message {}", (new io.github.luzzu.operations.lowlevel.Date()).getDate(), sparqlEndPoint, e.getMessage());
						ExceptionOutput.output(e, "Exception thrown whilst fetching triples from SPARQL Endpoint: "+ datasetLocation, logger);
						throw e;
					}
				}
			};
			_futureParser = executor.submit(parser);
		}
			
		executor.shutdown();
		

		try {
			while (!executor.isTerminated()){
				while (!(this.sparqlIterator.isEmpty())) {
					
					Object2Quad stmt = new Object2Quad(this.sparqlIterator.poll());
					sniffer.sniff(stmt.getStatement());
					
					if (lstMetricConsumers != null){
						for(MetricProcess mConsumer : lstMetricConsumers) {
							mConsumer.notifyNewQuad(stmt);
						}
					}
				}
			}
			_futureParser.get();
		
		} catch(RiotException | InterruptedException | ExecutionException e) {
			LuzzuIOException lio = new LuzzuIOException("Failed to process SPARQL endpoint: "+ sparqlEndPoint + " " + e.getMessage());
			logger.error("[SPARQL Endpoint Processor - {}] - Error parsing SPARQL Endpoint {}. Error message {}", (new io.github.luzzu.operations.lowlevel.Date()).getDate(), sparqlEndPoint, e.getMessage());
			
			ExceptionOutput.output(lio, "Exception thrown on SPARQL Endpoint: "+ datasetLocation, logger);
			throw lio;
		}
		finally {
			if (lstMetricConsumers != null){
				for(MetricProcess mConsumer : lstMetricConsumers) {
					mConsumer.stop();
				}	
			}		
		}
		
		countMetric.setCount(endpointSize);

		
		if (sniffer.getCachingObject() != null) {
			cacheMgr.addToCache(graphCacheName, sparqlEndPoint, sniffer.getCachingObject());
		}
		
		computeAfter();

	}

	@Override
	public void cleanUp() throws ProcessorNotInitialised {
		this.isInitalised = false;
		
		this.lstMetricConsumers.clear();
		this.metricInstances.clear();
				
		if (!this.executor.isShutdown()){
			this.executor.shutdownNow();
		}		
	}
	
	@Override
	public void cancelMetricAssessment() throws ProcessorNotInitialised {
		
	}
}