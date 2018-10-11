package io.github.luzzu.communications.exceptions;

public class ResourceNotFoundException extends Exception {

	private static final long serialVersionUID = -8870364322191035590L;

	public ResourceNotFoundException(String uuid) {
		super("No assessment initiated with ID "+uuid);
	}
}
