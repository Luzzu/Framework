package io.github.luzzu.exceptions;

@SuppressWarnings("serial")
public class SyntaxErrorException extends LuzzuIOException {

	public SyntaxErrorException(String message) {
		super(message);
	}
}
