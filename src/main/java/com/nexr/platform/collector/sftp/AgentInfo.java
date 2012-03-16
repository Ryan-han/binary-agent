package com.nexr.platform.collector.sftp;

public class AgentInfo {
	
	private String agentName;
	private String user;
	private String passwd;
	private String host;
	private int port;
	private String targetPath;
	private String type;
	
	
	
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getAgentName() {
		return agentName;
	}

	public void setAgentName(String agentName) {
		this.agentName = agentName;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPasswd() {
		return passwd;
	}

	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getTargetPath() {
		return targetPath;
	}

	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}

	@Override
	public String toString() {
		return "AgentInfo [agentName=" + agentName + ", user=" + user + ", passwd="
				+ passwd + ", host=" + host + ", port=" + port + ", targetPath="
				+ targetPath + ", type=" + type + "]";
	}

	

	
}
