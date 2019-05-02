package io.github.luzzu.io.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.Quad;

import io.github.luzzu.datatypes.Object2Quad;
import io.github.luzzu.exceptions.ExternalMetricLoaderException;
import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.exceptions.ProcessorNotInitialised;
import io.github.luzzu.exceptions.SyntaxErrorException;
import io.github.luzzu.io.AbstractIOProcessor;
import io.github.luzzu.io.helper.MetricProcess;
import io.github.luzzu.io.helper.StreamMetadataSniffer;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;

/**
 * @author Jeremy Debattista
 *
 */
public class StreamProcessor extends AbstractIOProcessor {
	
	final static Logger logger = LoggerFactory.getLogger(StreamProcessor.class);
	
	// RDFIterator parameters
	private final int rdfIterBufferSize = PipedRDFIterator.DEFAULT_BUFFER_SIZE * 2;
	private final int rdfIterPollTimeout = 1000;
	private final int rdfIterMaxPolls = 50;
	private final boolean rdfIterFairBufferLock = true;

	protected PipedRDFIterator<?> iterator;
	protected PipedRDFStream<?> rdfStream;
	
	

	public StreamProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration){
		super(datasetPLD, datasetLocation, genQualityReport, configuration);
		super.logger = StreamProcessor.logger;
	}
	
	public StreamProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration, String crawlDate){
		super(datasetPLD, datasetLocation, genQualityReport, configuration, crawlDate);
		super.logger = StreamProcessor.logger;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void setUpProcess() throws LuzzuIOException{
			Lang lang  = RDFLanguages.filenameToLang(datasetLocation);
	
			if ((lang == Lang.NQ) || (lang == Lang.NQUADS)){
				this.iterator = new PipedRDFIterator<Quad>(rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
				this.rdfStream = new PipedQuadsStream((PipedRDFIterator<Quad>) iterator);
			} else {
				this.iterator = new PipedRDFIterator<Triple>(rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
				this.rdfStream = new PipedTriplesStream((PipedRDFIterator<Triple>) iterator);
			}
			logger.debug("PipedRDFIterator initialized with: Buffer Size {}, Fair Lock {}, Poll Timeout {}, Max Polls {}", 
					rdfIterBufferSize, rdfIterFairBufferLock, rdfIterPollTimeout, rdfIterMaxPolls);
			
			
			try {
				loadMetrics();
	        	ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("streamprocessor-parser-thread-%d").build();
				this.executor = Executors.newSingleThreadExecutor(namedThreadFactory);
				this.isInitalised = true;
			} catch (ExternalMetricLoaderException e) {
				ExceptionOutput.output(e, "Error loading metrics", logger);
			}
	}
	
	public void startProcessing() throws InterruptedException, LuzzuIOException {
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	
		StreamMetadataSniffer sniffer = new StreamMetadataSniffer();
		
		Callable<Boolean> parser = new Callable<Boolean>(){
			@Override
			public Boolean call() throws Exception {
				try{
					RDFParser.source(datasetLocation).parse(rdfStream);
				} catch (Exception e) {
					ExceptionOutput.output(e, "Error parsing dataset: "+ datasetLocation, logger);
					signalSyntaxError.set(true);
					forcedCancel = true;
					throw new SyntaxErrorException(e.getMessage());
				}				
				return true;
			}
			
		};
				
		Future<Boolean> parserFuture = executor.submit(parser);
		
		long totalQuadsProcessed = 0;
		
		try {
			while (this.iterator.hasNext()) {
				totalQuadsProcessed++;				
				Object2Quad stmt = new Object2Quad(this.iterator.next());
				sniffer.sniff(stmt.getStatement());
				
				if (lstMetricConsumers != null){
					for (MetricProcess mConsumer : lstMetricConsumers) {
						// Notify each metric process about the new triple. Note that if the queue of triples to process of any metric process 
						// gets full, the call to notifyNewQuad() will block until space becomes available in that queue. This will in fact, 
						// block the triple distribution altogether (for all other metric processes too)
						try {
							mConsumer.notifyNewQuad(stmt);
						} catch(InterruptedException iex) {
							logger.warn("[Stream Processor - {}] - Processor Interrupted whilst assessing dataset: {}. Quads processed # : {}. Error details: {}", 
									(new io.github.luzzu.operations.lowlevel.Date()).getDate(), datasetLocation, totalQuadsProcessed, iex.getMessage());
							throw iex;
						}
					}
				}
			}
		} catch(RiotException rex) {
			logger.warn("[Stream Processor - {}] - Failed to process dataset: {}. RIOT Exception while attempting to process quad # : {}. Error details: {}", 
					(new io.github.luzzu.operations.lowlevel.Date()).getDate(), datasetLocation, totalQuadsProcessed, rex.getMessage());
			throw rex;
		} 
		finally {
			if (lstMetricConsumers != null) {
				for(MetricProcess mConsumer : lstMetricConsumers) {
					mConsumer.stop();
				}
			}		
		}

		if (sniffer.getCachingObject() != null) {
			cacheMgr.addToCache(graphCacheName, datasetLocation, sniffer.getCachingObject());
		}
		
		countMetric.setCount(totalQuadsProcessed);

		computeAfter();
		
		try {
			parserFuture.get();
		} catch (ExecutionException e) {
			if (e.getCause() instanceof SyntaxErrorException)
				throw new SyntaxErrorException(e.getMessage());
		}
	}
		
	@Override
	public void cancelMetricAssessment() throws ProcessorNotInitialised {
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	

		forcedCancel = true;
		
		for (MetricProcess mp : lstMetricConsumers){
			logger.info("Closing and clearing quads queue for "+ mp.getMetricName());
			mp.closeAssessment();
		}
		
		logger.info("Closing Iterators");
		try {
			this.iterator.close();
			this.rdfStream.finish();
		} catch (RiotException e){
			logger.warn("RDF Stream already closed. Exception Raised: {}",e.getMessage());
		}

		executor.shutdownNow();
	}
}
