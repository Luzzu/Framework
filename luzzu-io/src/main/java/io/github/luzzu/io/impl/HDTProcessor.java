package io.github.luzzu.io.impl;


import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdtjena.NodeDictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.datatypes.Object2Quad;
import io.github.luzzu.exceptions.ExternalMetricLoaderException;
import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.exceptions.ProcessorNotInitialised;
import io.github.luzzu.io.AbstractIOProcessor;
import io.github.luzzu.io.helper.MetricProcess;
import io.github.luzzu.io.helper.StreamMetadataSniffer;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;

public class HDTProcessor extends AbstractIOProcessor {
	
	final static Logger logger = LoggerFactory.getLogger(HDTProcessor.class);
	
	protected HDT processor;
	protected Dictionary hdtDictionary;
	protected NodeDictionary nodeDictionary;
	

	public HDTProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration) {
		super(datasetPLD, datasetLocation, genQualityReport, configuration);
		super.logger = HDTProcessor.logger;
	}
	
	public HDTProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration, String crawlDate){
		super(datasetPLD, datasetLocation, genQualityReport, configuration, crawlDate);
		super.logger = HDTProcessor.logger;
	}


	@Override
	public void setUpProcess() throws LuzzuIOException {
		try {
			this.processor = HDTManager.loadIndexedHDT(datasetLocation);
			this.hdtDictionary = this.processor.getDictionary();
			this.nodeDictionary = new NodeDictionary(this.hdtDictionary);
			logger.debug("Dataset {} is loaded into HDT processor.", datasetLocation);
		}  catch (IOException e) {
			ExceptionOutput.output(e, "Error loading HDT File", logger);
			throw new LuzzuIOException(e.getMessage());
		}
		
		try {
			loadMetrics();
			this.executor = Executors.newSingleThreadExecutor();
			this.isInitalised = true;
		} catch (ExternalMetricLoaderException e) {
			ExceptionOutput.output(e, "Error loading metrics", logger);
		}
	}
	
	
	@Override
	public void startProcessing() throws LuzzuIOException, InterruptedException {
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	
		StreamMetadataSniffer sniffer = new StreamMetadataSniffer();
		
		IteratorTripleString iter = null;
		try {
			iter = this.processor.search("", "", "");
		} catch (NotFoundException e) {
			ExceptionOutput.output(e, "HDT iterator error", logger);
		}
		
		if (iter != null) {
			logger.debug("Dataset {} is being parsed. Estimate number of triples {}", datasetLocation, iter.estimatedNumResults());
			
			long totalQuadsProcessed = 0;

			while (iter.hasNext()) {
				TripleString triple = iter.next();
				totalQuadsProcessed++;
				
				Node s = this.nodeDictionary.getNode(this.hdtDictionary.stringToId(triple.getSubject(), TripleComponentRole.SUBJECT), TripleComponentRole.SUBJECT);
				Node p = this.nodeDictionary.getNode(this.hdtDictionary.stringToId(triple.getPredicate(), TripleComponentRole.PREDICATE), TripleComponentRole.PREDICATE);
				Node o = this.nodeDictionary.getNode(this.hdtDictionary.stringToId(triple.getObject(), TripleComponentRole.OBJECT), TripleComponentRole.OBJECT);
				
				Triple t = new Triple(s,p,o);
				Object2Quad stmt = new Object2Quad(t);
				sniffer.sniff(stmt.getStatement());

				if (lstMetricConsumers != null){
					for (MetricProcess mConsumer : lstMetricConsumers) {
						try {
							mConsumer.notifyNewQuad(stmt);
						} catch(InterruptedException iex) {
							logger.warn("[HDT Processor - {}] - Processor Interrupted whilst assessing dataset: {}. Quads processed # : {}. Error details: {}", 
									(new io.github.luzzu.operations.lowlevel.Date()).getDate(), datasetLocation, totalQuadsProcessed, iex.getMessage());
							throw iex;
						}
					}
				}
			}
			
			countMetric.setCount(totalQuadsProcessed);
			
			if (lstMetricConsumers != null) {
				for(MetricProcess mConsumer : lstMetricConsumers) {
					mConsumer.stop();
				}
			}
			
			if (sniffer.getCachingObject() != null) {
				cacheMgr.addToCache(graphCacheName, datasetLocation, sniffer.getCachingObject());
			}
			
			computeAfter();
		} else {
			throw new ProcessorNotInitialised("Streaming will not start as HDT file seems to be empty");	
		}

	}

	@Override
	public void cancelMetricAssessment() throws LuzzuIOException {
		throw new UnsupportedOperationException("This action is not yet supported for the HDT processor");		
	}
}
