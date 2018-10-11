package io.github.luzzu.communications.utils;

import javax.ws.rs.core.Response;

public class APIResponse {

	public static Response ok(Object entity, String mType) {
		return Response.ok(entity, mType).header("Access-Control-Allow-Origin", "*")
				.header("Access-Control-Allow-Methods", "POST, GET, PUT, UPDATE, OPTIONS")
			      .header("Access-Control-Allow-Headers", "x-requested-with, x-requested-by").build();
	}
}
