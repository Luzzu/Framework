package io.github.luzzu.io.impl;

import java.util.Iterator;
import java.util.concurrent.Executors;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RiotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;

import io.github.luzzu.datatypes.Object2Quad;
import io.github.luzzu.exceptions.ExternalMetricLoaderException;
import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.exceptions.ProcessorNotInitialised;
import io.github.luzzu.exceptions.SyntaxErrorException;
import io.github.luzzu.io.AbstractIOProcessor;
import io.github.luzzu.io.helper.MetricProcess;
import io.github.luzzu.io.helper.StreamMetadataSniffer;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;

public class InMemoryProcessor extends AbstractIOProcessor {

	final static Logger logger = LoggerFactory.getLogger(InMemoryProcessor.class);
	
	
	private Dataset memoryModel = DatasetFactory.create();
	
	public InMemoryProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration){
		super(datasetPLD, datasetLocation, genQualityReport, configuration);
		super.logger = InMemoryProcessor.logger;
	}
	
		
	public InMemoryProcessor(String datasetPLD, String datasetLocation, boolean genQualityReport, Model configuration, String crawlDate){
		super(datasetPLD, datasetLocation, genQualityReport, configuration, crawlDate);
		super.logger = InMemoryProcessor.logger;
	}
	
	private void tryLoadModel() throws RiotException {
		try {
			this.memoryModel = RDFDataMgr.loadDataset(datasetLocation);
		} catch (RiotException re) {
			try {
				this.memoryModel = RDFDataMgr.loadDataset(datasetLocation, Lang.NTRIPLES);
			} catch (RiotException re1) {
				throw re1;
			}
		}
	}
	
	@Override
	public void setUpProcess() throws LuzzuIOException {
		try{
			tryLoadModel();
			loadMetrics();
			this.executor = Executors.newSingleThreadExecutor();
			this.isInitalised = true;
		} catch (RiotException | ExternalMetricLoaderException e) {
			ExceptionOutput.output(e, "Error parsing dataset: "+ datasetLocation, logger);
			if (e instanceof RiotException) {
				signalSyntaxError.set(true);
				forcedCancel = true;
				throw new SyntaxErrorException(e.getMessage());
			}
		}
	}

	@Override
	public void startProcessing() throws LuzzuIOException {
		if (!(signalSyntaxError.get())) {
			if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as memory processor has not been initalised");	
			StreamMetadataSniffer sniffer = new StreamMetadataSniffer();

			
			long totalQuadsProcessed = 0;
			
			try {
				Iterator<String> graphs = this.getMemoryModel().listNames();
				while(graphs.hasNext()){
					Model m = this.getMemoryModel().getNamedModel(graphs.next());
					for(Statement s : m.listStatements().toList()){
						totalQuadsProcessed++;
						Object2Quad stmt = new Object2Quad(s.asTriple());
						sniffer.sniff(stmt.getStatement());
						
						if (lstMetricConsumers != null){
							for (MetricProcess mConsumer : lstMetricConsumers) {
								try {
									mConsumer.notifyNewQuad(stmt);
								} catch(InterruptedException iex) {
									logger.warn("[Memory Processor - {}] - Processor Interrupted whilst assessing dataset: {}. Quads processed # : {}. Error details: {}", 
											(new io.github.luzzu.operations.lowlevel.Date()).getDate(), datasetLocation, totalQuadsProcessed, iex.getMessage());
									throw iex;
								}
							}
						}
					}
				}
				
				for(Statement s : this.getMemoryModel().getDefaultModel().listStatements().toList()){
					totalQuadsProcessed++;
					Object2Quad stmt = new Object2Quad(s.asTriple());
					sniffer.sniff(stmt.getStatement());
					
					if (lstMetricConsumers != null){
						for (MetricProcess mConsumer : lstMetricConsumers) {
							try {
								mConsumer.notifyNewQuad(stmt);
							} catch(InterruptedException iex) {
								logger.warn("[Memory Processor - {}] - Processor Interrupted whilst assessing dataset: {}. Quads processed # : {}. Error details: {}", 
										(new io.github.luzzu.operations.lowlevel.Date()).getDate(), datasetLocation, totalQuadsProcessed, iex.getMessage());
								throw iex;
							}
						}
					}
				}
			} catch(Exception ex) {
				logger.error("[Memory Processor - {}] Failed to process dataset: {}. Exception while attempting to process quad # : {}. Error details: {}", 
					(new io.github.luzzu.operations.lowlevel.Date()).getDate(), datasetLocation, totalQuadsProcessed, ex);
				throw new LuzzuIOException(ex.getMessage());
			}
			finally {
				if (lstMetricConsumers != null) {
					for(MetricProcess mConsumer : lstMetricConsumers) {
						mConsumer.stop();
					}
				}		
			}
			
			countMetric.setCount(totalQuadsProcessed);

			
			if (sniffer.getCachingObject() != null) {
				cacheMgr.addToCache(graphCacheName, this.datasetLocation, sniffer.getCachingObject());
			}
			
			computeAfter();
		}
	}

	@Override
	public void cleanUp() throws ProcessorNotInitialised {
		this.isInitalised = false;
		this.lstMetricConsumers.clear();
		this.metricInstances.clear();
	}

//	@Override
//	public void processorWorkFlow() throws LuzzuIOException{
//		this.setUpProcess();
//		
//		try {
//			this.startProcessing();
//		} catch (ProcessorNotInitialised e) {
//			logger.warn("[MemoryProcessor - {}]Processor not initialized while trying to start dataset processing. Dataset: {}", 
//						(new io.github.luzzu.commons.Date()).getDate(), this.datasetLocation);
//			this.processorWorkFlow();
//		}
//		
//		if (!forcedCancel){
//			this.generateAndWriteQualityMetadataReport();
//			if (this.genQualityReport) {
//				this.generateAndWriteQualityProblemReport();
//			} else {
//				this.clearTDBFiles();
//			}
//		} else {
//			this.clearTDBFiles();
//		}
//	}
	
	public Dataset getMemoryModel() {
		return memoryModel;
	}

	public void setMemoryModel(Dataset memoryModel) {
		this.memoryModel = memoryModel;
	}

	public void cancelMetricAssessment() throws LuzzuIOException {
		if(this.isInitalised == false) throw new ProcessorNotInitialised("Streaming will not start as processor has not been initalised");	
		
		forcedCancel = true;
		
		for (MetricProcess mp : lstMetricConsumers){
			logger.info("Closing and clearing quads queue for {}", mp.getMetricName());
			mp.closeAssessment();
		}
		
		logger.info("Closing Iterators");
		this.getMemoryModel().close();
	}
}
