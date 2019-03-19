package io.github.luzzu.operations.lowlevel;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

public class ExceptionOutput {

	public static void output(Exception e, String errorHeader, final Logger logger) {
		System.out.printf("[Error - %s] %s: %s\n", errorHeader, (new Date()).getDate(), e.getMessage());
		String stackTrace = "";
		if (e.getCause() != null) stackTrace = ExceptionUtils.getStackTrace(e.getCause());
		logger.error("[{} - {}] Exception raised in Luzzu: \n{}", errorHeader, (new Date()).getDate(), stackTrace);
	}
}
