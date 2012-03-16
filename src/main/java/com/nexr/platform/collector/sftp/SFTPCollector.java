package com.nexr.platform.collector.sftp;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.nexr.platform.collector.util.ExponentialBackoff;

public class SFTPCollector {

	private static Logger log = Logger.getLogger(SFTPCollector.class);
	private static final String SOURCE_DIR = "sourceDir";
	private static final String SOURCE_SUFFIX = "sourceSuffix";
	private static final String SOURCE_CHECK_PERIOD = "sourceCheckPeriod";
	private static final String COPY_PERIOD = "copyPeriod";
	private static final String SOURCE_RETENTION = "sourceRetention";
	private static final String SOURCE_RETENTION_DIR = "sourceRetentionDir";

	private static final String BACKOFF_CEILING = "backoff.ceiling";
	private static final String BACKOFF_MAX = "backoff.max";

	private String sourceSuffix = null;
	private File sourcePath;
	private List<String> sourceList = new CopyOnWriteArrayList<String>();
	private List<AgentInfo> agentList = new CopyOnWriteArrayList<AgentInfo>();
	private ConcurrentHashMap<String, ChannelSftp> channelMap = new ConcurrentHashMap<String, ChannelSftp>();

	boolean copySuccess = false;
	ExponentialBackoff copyBackoff;

	SourceCheckThread sourceCheckThread;
	CopyThread copyThread;

	long sourceCheckPeriod = 5000;
	long copyPeriod = 5000;

	String sourceRetention = "rename";
	String sourceRetentionDir = null;

	long backoff_ceiling = 1000;
	long backoff_max = 50;

	public static void main(String args[]) {
		SFTPCollector sftpCollector = new SFTPCollector();

		sftpCollector.init();
		sftpCollector.start();
	}

	public void start() {
		sourceCheckThread = new SourceCheckThread();
		copyThread = new CopyThread();

		sourceCheckThread.start();
		copyThread.start();
	}

	public void init() {
		// InputStream is = this.getClass().getResourceAsStream("conf.properties");
		InputStream is = null;
		try {
			is = new FileInputStream(
					new File(
							"/Users/ryan/workspaces/dev-nexr-collector/binary-agent/src/main/resources/conf.properties"));
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
				} else if (key.equals(SOURCE_RETENTION_DIR)) {
					if (prop.getProperty(key) != null
							|| prop.getProperty(key).trim().length() > 0) {
						sourceRetentionDir = prop.getProperty(key).trim();

						File sourceRetentionPath = new File(sourceRetentionDir);
						if (!sourceRetentionPath.exists()) {
							sourceRetentionPath.mkdirs();
						}
						log.info("Retention Directory " + sourceRetentionDir);
					}

				} else {
					AgentInfo agentInfo = getAgentInfo(key, prop.getProperty(key));
					if (agentInfo != null) {
						agentList.add(agentInfo);
						log.info("Agent addded " + agentInfo.toString());
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
		if (infos.length != 5) {
			log.error("Invalid agent info so ignored Key : " + key);
			return null;
		}

		AgentInfo agentInfo = new AgentInfo();
		agentInfo.setAgentName(key);
		agentInfo.setUser(infos[0]);
		agentInfo.setPasswd(infos[1]);
		agentInfo.setHost(infos[2]);
		agentInfo.setPort(Integer.parseInt(infos[3]));
		agentInfo.setTargetPath(infos[4]);
		return agentInfo;
	}

	public ChannelSftp getChannel(AgentInfo agentInfo) {
		Session session;
		Channel channel;
		ChannelSftp sftp = null;

		if (channelMap.contains(agentInfo.getAgentName())) {
			sftp = channelMap.get(agentInfo.getAgentName());
		} else {

			JSch jsch = new JSch();
			try {
				session = jsch.getSession(agentInfo.getUser(), agentInfo.getHost(),
						agentInfo.getPort());

				session.setUserInfo(new PermissionInfo(agentInfo.getPasswd()));
				session.connect();
				
				channel = session.openChannel("ftp");
				channel.connect();
				sftp = (ChannelSftp) channel;
				channelMap.put(agentInfo.getAgentName(), sftp);

			} catch (JSchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return sftp;
	}

	public void copyFiles(String source, AgentInfo agentInfo)
			throws SftpException {
		ChannelSftp sftp = getChannel(agentInfo);

		sftp.cd(agentInfo.getTargetPath());

		try {

			// .FIN이 없는 데이타 파일 복
			String notFin = source.substring(0, source.lastIndexOf("."));
			sftp.put(new FileInputStream(notFin),
					notFin.substring(notFin.lastIndexOf("/") + 1, notFin.length()));
			log.info("Source " + notFin + " Agent " + agentInfo.getAgentName());

			// .FIN file copy
			log.info("Source " + source + " Agent " + agentInfo.getAgentName());
			sftp.put(new FileInputStream(new File(source)),
					source.substring(source.lastIndexOf("/") + 1, source.length()));

			retain(sourceRetention, source);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void retain(String type, String source) {
		if (sourceRetention.equals("rename")) {
			File file = new File(source.substring(0, source.lastIndexOf(".")));
			file.renameTo(new File(sourceRetentionDir + "/" + file.getName()
					+ ".copyed"));
			file = new File(source);
			file.renameTo(new File(sourceRetentionDir + "/" + file.getName()
					+ ".copyed"));
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

						while (!copySuccess) {

							int retries = 0;
							int index = idxKey % agentList.size();
							AgentInfo agentInfo = agentList.get(index);
							try {
								copyFiles(source, agentInfo);
								copySuccess = true;
							} catch (Exception e) {
								// TODO Auto-generated catch block
								idxKey++;
								long waitTime = copyBackoff.sleepIncrement();
								log.info("attempt " + retries + " failed, backoff (" + waitTime
										+ "ms): " + e.getMessage());

								copyBackoff.backoff();

								try {
									copyBackoff.waitUntilRetryOk();
								} catch (InterruptedException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
								retries++;
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

					}

					sourceList.remove(0);
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
				log.info("New file added " + file.getAbsolutePath());
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
	
	public String getFlumeConfDir() {
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
}
