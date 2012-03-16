package com.nexr.platform.collector.sftp;

import com.jcraft.jsch.UserInfo;

public class PermissionInfo implements UserInfo {

	private String password;

	public PermissionInfo(String password) {
		this.password = password;
	}

	public String getPassphrase() {
		return null;
	}

	public String getPassword() {
		return password;
	}

	public boolean promptPassphrase(String arg0) {
		return false;
	}

	public boolean promptPassword(String arg0) {
		return true;
	}

	public boolean promptYesNo(String arg0) {
		return true;
	}

	public void showMessage(String arg0) {
	}

}
