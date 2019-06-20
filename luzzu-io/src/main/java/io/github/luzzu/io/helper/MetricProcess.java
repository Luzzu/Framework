package io.github.luzzu.io.helper;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.github.luzzu.assessment.QualityMetric;
import io.github.luzzu.datatypes.Object2Quad;
import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;

public final class MetricProcess {

	private volatile BlockingQueue<Object2Quad> quadsToProcess = new ArrayBlockingQueue<Object2Quad>(5000000); // 500000
	// private Thread metricThread = null;
	private String metricName = null;
	// private MetricProcessingException thrownException = null;
	private Logger logger = null;

	private ExecutorService executor = null;

	private Future<Boolean> successfulProcessing = null;

	Long stmtsProcessed = 0l;
	boolean stopSignal = false;
	AtomicBoolean threadFinished = new AtomicBoolean(false);

	public MetricProcess(final Logger logger, final QualityMetric<?> m) {
		this.logger = logger;
		this.metricName = m.getClass().getSimpleName();

		ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat(this.metricName + "-thread-%d").build();
		this.executor = Executors.newSingleThreadExecutor(namedThreadFactory);

		Callable<Boolean> mProcess = new Callable<Boolean>() {
			@Override
			public Boolean call() throws MetricProcessingException {
				logger.debug("Starting thread for metric {}", m.getClass().getName());

				Object2Quad curQuad = null;
				while (!stopSignal || !quadsToProcess.isEmpty()) {
					curQuad = quadsToProcess.poll();

					if (curQuad != null) {
						logger.trace("Metric {}, new quad (processed: {}, to-process: {})", m.getClass().getName(), stmtsProcessed, quadsToProcess.size());

						try {
							m.compute(curQuad.getStatement());
							curQuad = null;
							stmtsProcessed++;
							threadFinished.set(true);
						} catch (MetricProcessingException e) {
							ExceptionOutput.output(e, "Halting metric processing " + metricName + ". Quad causing problem: " + curQuad.getStatement().toString(), logger);
							stopSignal = true;
							throw e;
						}
					}
				}
				logger.debug("Thread for metric {} completed, total statements processed {}", m.getClass().getName(), stmtsProcessed);
				return true;
			}

		};

		successfulProcessing = executor.submit(mProcess);
	}

	public void notifyNewQuad(Object2Quad newQuad) throws InterruptedException {
		// Try to set the incoming triple into the blocking queue, so that if
		// its full, this thread blocks (i.e. waits) until space is available in the queue.

		quadsToProcess.put(newQuad);
		logger.trace("Metric {}, element put into queue (to-process: {})", this.metricName, quadsToProcess.size());
	}

	public void stop() {
		while ((!quadsToProcess.isEmpty()) && (!threadFinished.get())) {
			logger.trace("Waiting for items on queue: {} Metric: {}", quadsToProcess.size(), this.metricName);
		}

		this.stopSignal = true;
	}

	public Long getStatementProcessed() {
		return this.stmtsProcessed;
	}

	public String getMetricName() {
		return this.metricName;
	}

	public void closeAssessment() {
		this.stopSignal = true;
		this.quadsToProcess.clear();
	}

	public boolean isDoneParsing() {
		return quadsToProcess.isEmpty();
	}

	public boolean isSuccessfulParsing() {
		boolean result = false;
		try {
			result = successfulProcessing.get();
		} catch (Exception e) {
			// do nothing with this exception
		}

		return result;
	}
}
