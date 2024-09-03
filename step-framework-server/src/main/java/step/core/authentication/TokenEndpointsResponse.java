package step.core.authentication;

import java.util.HashMap;

public class TokenEndpointsResponse extends HashMap<String,Object> {
	public final static String ACCESS_TOKEN_KEY = "access_token";
	public final static String ID_TOKEN_KEY = "id_token";
	public final static String REFRESH_TOKEN = "refresh_token";

	public TokenEndpointsResponse() {
		super();
	}

	public TokenEndpointsResponse(String accessToken) {
		super();
		this.put(ACCESS_TOKEN_KEY, accessToken);
	}

	public String getAccessToken() {
		return (String) this.get(ACCESS_TOKEN_KEY);
	}

	public String getIdToken() {
		return (String) this.get(ID_TOKEN_KEY);
	}

	public String getRefreshToken() {
		return (String) this.get(REFRESH_TOKEN);
	}
}
