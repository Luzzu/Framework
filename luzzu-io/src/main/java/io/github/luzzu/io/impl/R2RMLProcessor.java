package io.github.luzzu.io.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RiotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.assessment.ComplexQualityMetric;
import io.github.luzzu.datatypes.Object2Quad;
import io.github.luzzu.datatypes.r2rml.R2RMLMapping;
import io.github.luzzu.datatypes.r2rml.R2RMLMappingFactory;
import io.github.luzzu.exceptions.BeforeException;
import io.github.luzzu.exceptions.ExternalMetricLoaderException;
import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.exceptions.ProcessorNotInitialised;
import io.github.luzzu.exceptions.SyntaxErrorException;
import io.github.luzzu.io.AbstractIOProcessor;
import io.github.luzzu.io.helper.MetricProcess;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;

public class R2RMLProcessor extends AbstractIOProcessor {

	final static Logger logger = LoggerFactory.getLogger(R2RMLProcessor.class);

	private Model mappingModel = ModelFactory.createDefaultModel();
	private List<R2RMLMapping> r2rmlMappings = Collections.synchronizedList(new ArrayList<R2RMLMapping>());

	public R2RMLProcessor(String mappingURIID, String mappingFileLocation, boolean genQualityReport, Model configuration) {
		super(mappingURIID, mappingFileLocation, genQualityReport, configuration);
		super.logger = R2RMLProcessor.logger;
	}

	public R2RMLProcessor(String mappingURIID, String mappingFileLocation, boolean genQualityReport, Model configuration, String crawlDate) {
		super(mappingURIID, mappingFileLocation, genQualityReport, configuration, crawlDate);
		super.logger = R2RMLProcessor.logger;
	}

	private void tryLoadMapping() throws RiotException {
		try {
			this.mappingModel = RDFDataMgr.loadModel(datasetLocation);
		} catch (RiotException re) {
			try {
				this.mappingModel = RDFDataMgr.loadModel(datasetLocation, Lang.TURTLE);
			} catch (RiotException re1) {
				throw re1;
			}
		}

		try {
			r2rmlMappings.add(R2RMLMappingFactory.createR2RMLMappingFromModel(this.mappingModel));
		} catch (Exception e) {
			logger.error("Error processing R2RML mapping from file.");
			throw e;
		}
	}

	@Override
	public void setUpProcess() throws LuzzuIOException {
		try {
			tryLoadMapping();
			loadMetrics();
			this.executor = Executors.newSingleThreadExecutor();
			this.isInitalised = true;
		} catch (RiotException | ExternalMetricLoaderException e) {
			ExceptionOutput.output(e, "Error parsing mapping file: " + datasetLocation, logger);
			if (e instanceof RiotException) {
				signalSyntaxError.set(true);
				forcedCancel = true;
				throw new SyntaxErrorException(e.getMessage());
			}
		}
	}

	@Override
	protected void initiateMetricProcessThreads() {
		for (String className : this.metricInstances.keySet()) {
			if (this.metricInstances.get(className) instanceof ComplexQualityMetric<?>) {
				try {
					((ComplexQualityMetric<?>) this.metricInstances.get(className)).before(r2rmlMappings);
				} catch (BeforeException e) {
					ExceptionOutput.output(e, "Error initiating metric process threads with R2RML Configuration: " + datasetLocation, logger);
					// logger.error(e.getMessage());
				}
			}
			this.lstMetricConsumers.add(new MetricProcess(logger, this.metricInstances.get(className)));
		}
	}

	@Override
	public void startProcessing() throws LuzzuIOException, InterruptedException {
		if (!(signalSyntaxError.get())) {
			if (this.isInitalised == false)
				throw new ProcessorNotInitialised("Streaming will not start as R2RML processor has not been initalised");

			long totalMappingTriplesProcessed = this.mappingModel.size();

			if (lstMetricConsumers != null) {
				for (MetricProcess mConsumer : lstMetricConsumers) {
					try {
						Triple empty = new Triple(this.mappingModel.createResource().asNode(), this.mappingModel.createProperty("http://blank.org").asNode(),
								this.mappingModel.createResource().asNode());
						Object2Quad stmt = new Object2Quad(empty);
						mConsumer.notifyNewQuad(stmt);

					} catch (InterruptedException iex) {
						logger.warn("[R2RML Processor - {}] - Processor Interrupted whilst assessing dataset: {}. Error details: {}", (new io.github.luzzu.operations.lowlevel.Date()).getDate(),
								datasetLocation, iex.getMessage());
						throw iex;
					}
				}
			}

			if (lstMetricConsumers != null) {
				for (MetricProcess mConsumer : lstMetricConsumers) {
					mConsumer.stop();
				}
			}

			countMetric.setCount(totalMappingTriplesProcessed);

			computeAfter();
		}
	}

	@Override
	public void cancelMetricAssessment() throws LuzzuIOException {
		if (this.isInitalised == false)
			throw new ProcessorNotInitialised("R2RML processor will not start as processor has not been initalised");

		forcedCancel = true;

		for (MetricProcess mp : lstMetricConsumers) {
			logger.info("Closing and clearing quads queue for {}", mp.getMetricName());
			mp.closeAssessment();
		}

		logger.info("Closing and cleaning mapping model");
		this.getMappingModel().close();
		this.cleanUp();
	}

	@Override
	public void cleanUp() throws ProcessorNotInitialised {
		this.isInitalised = false;
		this.lstMetricConsumers.clear();
		this.metricInstances.clear();
		this.r2rmlMappings.clear();
	}

	public Model getMappingModel() {
		return this.mappingModel;
	}

	public void setMappingModel(Model mappingModel) {
		this.mappingModel = mappingModel;
	}
}
