package io.github.luzzu.qualitymetrics.commons.utils;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

import io.github.luzzu.operations.lowlevel.Date;

public class ExceptionOutput {

	public static void output(Exception e, String errorHeader, final Logger logger) {
		System.out.println();
		System.out.printf("[Error - %s] %s: %s\n", errorHeader, (new Date()).getDate(), e.getMessage());
		String stackTrace = ExceptionUtils.getStackTrace(e.getCause());
		logger.error("[{} - {}] Exception raised in Luzzu: \n{}", errorHeader, (new Date()).getDate(), stackTrace);
	}
}
