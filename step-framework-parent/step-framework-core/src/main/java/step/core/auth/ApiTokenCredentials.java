package step.core.auth;

public class ApiTokenCredentials extends AbstractCredentials {
	private String token;

	public ApiTokenCredentials(String token) {
		this.token = token;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}
}
