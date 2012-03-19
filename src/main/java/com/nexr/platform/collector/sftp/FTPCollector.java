package com.nexr.platform.collector.sftp;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.nexr.platform.collector.util.ExponentialBackoff;

public class FTPCollector {

	private static Logger log = Logger.getLogger(FTPCollector.class);
	private static final String SOURCE_DIR = "sourceDir";
	private static final String SOURCE_SUFFIX = "sourceSuffix";
	private static final String SOURCE_CHECK_PERIOD = "sourceCheckPeriod";
	private static final String COPY_PERIOD = "copyPeriod";

	private static final String SOURCE_RETENTION = "sourceRetention";
	private static final String SOURCE_RETENTION_DIR = "sourceRetentionDir";
	private static final String SOURCE_RETENTION_PERIOD = "sourceRetentionPeriod";

	private static final String BACKOFF_CEILING = "backoff.ceiling";
	private static final String BACKOFF_MAX = "backoff.max";

	private String sourceSuffix = null;
	private File sourcePath;
	private List<String> sourceList = new CopyOnWriteArrayList<String>();
	private List<AgentInfo> agentList = new CopyOnWriteArrayList<AgentInfo>();
	private ConcurrentHashMap<String, Object> channelMap = new ConcurrentHashMap<String, Object>();

	boolean copySuccess = false;
	ExponentialBackoff copyBackoff;

	SourceCheckThread sourceCheckThread;
	CopyThread copyThread;
	SourceRetainThread sourceRetainThread;

	long sourceCheckPeriod = 5000;
	long copyPeriod = 5000;

	String sourceRetention = "rename";
	String sourceRetentionDir = null;
	File sourceRetentionPath;
	int sourceRetentionPeriod = -15;

	long backoff_ceiling = 1000;
	long backoff_max = 50;

	static FTPCollector ftpCollector;

	public static void main(String args[]) {
		ftpCollector = new FTPCollector();
		ftpCollector.init();
		ftpCollector.start();

	}

	public void start() {
		sourceCheckThread = new SourceCheckThread();
		copyThread = new CopyThread();
		sourceRetainThread = new SourceRetainThread();

		sourceCheckThread.start();
		copyThread.start();
		sourceRetainThread.start();

	}

	public void init() {
		InputStream is = null;
		try {
			is = new FileInputStream(new File(getConfDir() + "/conf.properties"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Properties prop = new Properties();
		try {
			prop.load(is);

			Enumeration<Object> enu = prop.keys();
			while (enu.hasMoreElements()) {
				String key = enu.nextElement().toString();
				if (key.equals(SOURCE_DIR)) {
					String source = prop.getProperty(key).trim();
					if (source == null || source.trim().length() == 0) {
						log.error("Source Path is not set so shutdown");
					} else {
						sourcePath = new File(source);
						log.info("Source Path is " + sourcePath);
					}
				} else if (key.equals(BACKOFF_CEILING)) {
					if (prop.getProperty(key) != null
							|| prop.getProperty(key).trim().length() > 0) {
						backoff_ceiling = Long.parseLong(prop.getProperty(key).trim());
						log.info("Backoff ceiling " + backoff_ceiling);
					}
				} else if (key.equals(BACKOFF_MAX)) {
					if (prop.getProperty(key) != null
							|| prop.getProperty(key).trim().length() > 0) {
						backoff_max = Long.parseLong(prop.getProperty(key).trim());
						log.info("Backoff max " + backoff_max);
					}
				} else if (key.equals(SOURCE_SUFFIX)) {
					if (prop.getProperty(key) != null
							|| prop.getProperty(key).trim().length() > 0) {
						sourceSuffix = prop.getProperty(key).trim();
						log.info("Source suffix set " + prop.getProperty(key));
					}
				} else if (key.equals(SOURCE_CHECK_PERIOD)) {
					if (prop.getProperty(key) != null
							|| prop.getProperty(key).trim().length() > 0) {
						sourceCheckPeriod = Long.parseLong(prop.getProperty(key).trim());
						log.info("Source Check Period " + sourceCheckPeriod);
					}
				} else if (key.equals(COPY_PERIOD)) {
					if (prop.getProperty(key) != null
							|| prop.getProperty(key).trim().length() > 0) {
						copyPeriod = Long.parseLong(prop.getProperty(key).trim());
						log.info("Copy period " + copyPeriod);
					}
				} else if (key.equals(SOURCE_RETENTION)) {
					if (prop.getProperty(key) != null
							|| prop.getProperty(key).trim().length() > 0) {
						sourceRetention = prop.getProperty(key).trim();
						log.info("Retention policy " + sourceRetention);

					}
				} else if (key.equals(SOURCE_RETENTION_PERIOD)) {
					if (prop.getProperty(key) != null
							|| prop.getProperty(key).trim().length() > 0) {
						sourceRetentionPeriod = Integer.parseInt(prop.getProperty(key)
								.trim());
						// int var = sourceRetentionPeriod * 2;
						// sourceRetentionPeriod = sourceRetentionPeriod - var;
						log.info("Retention Period " + sourceRetentionPeriod);

					}
				} else if (key.equals(SOURCE_RETENTION_DIR)) {
					if (prop.getProperty(key) != null
							|| prop.getProperty(key).trim().length() > 0) {
						sourceRetentionDir = prop.getProperty(key).trim();

						sourceRetentionPath = new File(sourceRetentionDir);
						if (!sourceRetentionPath.exists()) {
							sourceRetentionPath.mkdirs();
						}
						log.info("Retention Directory " + sourceRetentionDir);
					}

				} else {
					AgentInfo agentInfo = getAgentInfo(key, prop.getProperty(key));
					if (agentInfo != null) {
						agentList.add(agentInfo);
						log.info("Agent added " + agentInfo.toString());
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		copyBackoff = new ExponentialBackoff(backoff_ceiling, backoff_max);
	}

	public AgentInfo getAgentInfo(String key, String info) {
		if (info == null) {
			return null;
		}
		String[] infos = info.trim().split(",");
		if (infos.length != 6) {
			log.error("Invalid agent info so ignored Key : " + key);
			return null;
		}

		AgentInfo agentInfo = new AgentInfo();
		agentInfo.setAgentName(key);
		agentInfo.setType(infos[0]);
		agentInfo.setUser(infos[1]);
		agentInfo.setPasswd(infos[2]);
		agentInfo.setHost(infos[3]);
		agentInfo.setPort(Integer.parseInt(infos[4]));
		agentInfo.setTargetPath(infos[5]);
		return agentInfo;
	}

	Session session;
	Channel channel;

	public Object getChannel(AgentInfo agentInfo) throws JSchException,
			SocketException, IOException, SftpException {

		Object ftp = null;

		if (channelMap.get(agentInfo.getAgentName()) != null) {
			ftp = channelMap.get(agentInfo.getAgentName());
			log.debug("Get connection from map " + agentInfo.getAgentName());
		} else {
			log.info("Create connection for " + agentInfo.getAgentName());
			if (agentInfo.getType().toLowerCase().equals("sftp")) {
				JSch jsch = new JSch();

				session = jsch.getSession(agentInfo.getUser(), agentInfo.getHost(),
						agentInfo.getPort());

				session.setUserInfo(new PermissionInfo(agentInfo.getPasswd()));
				session.connect();

				channel = session.openChannel("sftp");
				channel.connect();
				ChannelSftp sftp = (ChannelSftp) channel;
				sftp.cd(agentInfo.getTargetPath());
				ftp = (ChannelSftp) sftp;

				channelMap.put(agentInfo.getAgentName(), ftp);

			} else if (agentInfo.getType().toLowerCase().equals("ftp")) {
				FTPClient f = new FTPClient();

				f.setControlEncoding("UTF-8");
				f.connect(agentInfo.getHost());
				f.login(agentInfo.getUser(), agentInfo.getPasswd());
				f.enterLocalPassiveMode(); // Passive Mode 접속일때
				f.changeWorkingDirectory(agentInfo.getTargetPath()); // 작업 디렉토리 변경
				f.setFileType(FTP.BINARY_FILE_TYPE);

				ftp = (FTPClient) f;

				channelMap.put(agentInfo.getAgentName(), ftp);
			}
		}
		return ftp;
	}

	public void copyFiles(File notFinFile, File finFile, String source, AgentInfo agentInfo)
			throws SftpException, IOException, JSchException {
		// .FIN이 없는 데이타 파일 복
		String notFin = source.substring(0, source.lastIndexOf("."));
//		// .FIN file copy
		String fin = source.substring(source.lastIndexOf("/") + 1, source.length());
//
//		File notFinFile = new File(notFin);
//		File finFile = new File(source);

		log.info("File Upload to " + agentInfo.toString());
		if (getChannel(agentInfo) instanceof ChannelSftp) {

			ChannelSftp sftp = (ChannelSftp) getChannel(agentInfo);
			sftp.cd(agentInfo.getTargetPath());
			FileInputStream is = new FileInputStream(notFinFile);
			sftp.put(is,
					notFin.substring(notFin.lastIndexOf("/") + 1, notFin.length()));
			is.close();
			log.debug("Source " + notFin + " Agent " + agentInfo.getAgentName());
			is = new FileInputStream(finFile);
			sftp.put(is, fin);
			is.close();
			log.debug("Source " + source + " Agent " + agentInfo.getAgentName());
		} else if (getChannel(agentInfo) instanceof FTPClient) {
			FTPClient ftp = (FTPClient) getChannel(agentInfo);
			// File notFinFile = new File(notFin);
			// File finFile = new File(source);

			FileInputStream fis = new FileInputStream(notFinFile);
			ftp.storeFile(notFinFile.getName(), fis);
			fis.close();
			log.debug("Source " + notFin + " Agent " + agentInfo.getAgentName());

			fis = new FileInputStream(finFile);
			ftp.storeFile(finFile.getName(), fis);
			fis.close();
			log.debug("Source " + source + " Agent " + agentInfo.getAgentName());
		}

		log.info(source + " Copy to " + agentInfo.getAgentName() + " Success");
		retain(sourceRetention, source);

		sourceList.remove(source);
	}

	private void retain(String type, String source) {
		if (sourceRetention.equals("rename")) {
			File retainSourcePath = new File(sourceRetentionPath, getRetainSubdir(0));
			if (!retainSourcePath.exists()) {
				retainSourcePath.mkdirs();
			}

			File file = new File(source.substring(0, source.lastIndexOf(".")));
			file.renameTo(new File(retainSourcePath, file.getName()));
			file = new File(source);
			file.renameTo(new File(retainSourcePath, file.getName()));
		} else if (sourceRetention.equals("delete")) {
			File file = new File(source.substring(0, source.lastIndexOf(".")));
			file.delete();
			file = new File(source);
			file.delete();
		}
	}

	class CopyThread extends Thread {

		public CopyThread() {
			// TODO Auto-generated constructor stub
			super("CopyThread");
		}

		public void run() {
			int idxKey = 0;
			while (true) {
				if (sourceList.size() > 0 && agentList.size() > 0) {
					for (String source : sourceList) {
						String notFin = source.substring(0, source.lastIndexOf("."));
				
						File notFinFile = new File(notFin);
						File finFile = new File(source);

						if (notFinFile.exists() && finFile.exists()) {
							int retries = 0;
							while (!copySuccess) {

								int index = idxKey % agentList.size();
								AgentInfo agentInfo = agentList.get(index);
								try {
									copyFiles(notFinFile, finFile, source, agentInfo);
									copySuccess = true;
								} catch (Exception e) {
									// TODO Auto-generated catch block
									idxKey++;
									long waitTime = copyBackoff.sleepIncrement();
									log.error(agentInfo.getAgentName() + " copy failed " + source
											+ " So Attempt " + retries + " failed, backoff ("
											+ waitTime + "ms): " + e.getMessage());

									copyBackoff.backoff();

									try {
										copyBackoff.waitUntilRetryOk();
									} catch (InterruptedException e1) {
										// TODO Auto-generated catch block
										e1.printStackTrace();
									}
									retries++;

									channelMap.remove(agentInfo.getAgentName());
									log.debug("Remove " + agentInfo.getAgentName()
											+ " Connection from cache");
								}

								if (copySuccess) {
									copyBackoff.reset();
									copySuccess = false;
									break;
								}
								if (copyBackoff.isFailed()) {
									// 시스템 종
									log.error("System have a problem so shutdown !");
									System.exit(0);
								}
							}
						} else {
							sourceList.remove(source);
						}

					}
					idxKey++;
				}

				try {
					Thread.sleep(copyPeriod);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public void checkSource() {
		log.debug("check Source " + sourcePath);
		File[] fileList = sourcePath.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				log.debug("Searched Source " + pathname.getName());
				// TODO Auto-generated method stub
				if (sourceSuffix == null) {
					return true;
				} else {
					if (pathname.getAbsolutePath().endsWith(sourceSuffix)) {
						return true;
					}
					return false;
				}
			}
		});

		for (File file : fileList) {
			if (!sourceList.contains(file.getAbsolutePath())) {
				sourceList.add(file.getAbsolutePath());
				log.debug("New file added " + file.getAbsolutePath());
			}
		}
	}

	public void sourceRetain() {
		String targetDir = getRetainSubdir(sourceRetentionPeriod);

		log.debug("check Retain files " + sourceRetentionDir);
		File[] fileList = sourceRetentionPath.listFiles();

		for (File file : fileList) {
			if (file.getName().startsWith(targetDir)) {
				if (file.delete()) {
					log.info("Delete " + file.getName()
							+ " because it has over the retention time");
				} else
					log.info("Delete fail " + file.getName());

			}
		}
	}

	class SourceRetainThread extends Thread {

		public SourceRetainThread() {
			// TODO Auto-generated constructor stub
			super("SourceRetainThread");
		}

		public void run() {
			while (true) {
				sourceRetain();
				try {
					Thread.sleep(60000 * 60);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	class SourceCheckThread extends Thread {

		public SourceCheckThread() {
			// TODO Auto-generated constructor stub
			super("SourceCheckThread");
		}

		public void run() {
			while (true) {
				checkSource();
				try {
					Thread.sleep(sourceCheckPeriod);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public String getConfDir() {
		String agentConfDir = System.getProperty("agent.conf.dir");
		if (null == agentConfDir) {
			agentConfDir = System.getenv("AGENT_CONF_DIR");
		}

		if (null == agentConfDir) {
			String agentHome = getAgentHome();
			if (null != agentHome) {
				agentConfDir = new File(agentHome, "conf").toString();
			} else {
				agentConfDir = "./conf";
			}
		}

		return agentConfDir;
	}

	public static String getAgentHome() {
		String agentHome = System.getProperty("agent.home");
		if (null == agentHome) {
			agentHome = System.getenv("AGENT_HOME");
		}

		if (null == agentHome) {
			log.warn("-Dagent.home and $AGENT_HOME both unset");
		}

		return agentHome;
	}

	synchronized private String getRetainSubdir(int amount) {
		Calendar c = Calendar.getInstance();
		if (amount < 0) {
			c.add(Calendar.DATE, amount);
		}
		int year = c.get(Calendar.YEAR);
		int month = c.get(Calendar.MONTH) + 1;
		int day = c.get(Calendar.DATE);
		int hour = c.get(Calendar.HOUR_OF_DAY);

		StringBuilder sb = new StringBuilder();
		sb.append(year + "-");
		if (month < 10) {
			sb.append("0" + month + "-");
		} else {
			sb.append(month + "-");
		}
		if (day < 10) {
			sb.append("0" + day + "-");
		} else {
			sb.append(day + "-");
		}
		if (amount == 0) {
			if (hour < 10) {
				sb.append("0" + hour);
			} else {
				sb.append(hour);
			}
		}

		return sb.toString();

	}

}
