package io.github.luzzu.datatypes.r2rml;

/**
 * R2RMLException Class.
 * 
 * Based on code developed by Christophe Debruyne (https://opengogs.adaptcentre.ie/debruync/r2rml)
 * released under MIT license
 *
 */
public class R2RMLException extends Exception {

	private static final long serialVersionUID = 1L;

	public R2RMLException(String message, Throwable throwable) {
		super(message, throwable);
	}

	public R2RMLException(String message) {
		super(message);
	}

}
