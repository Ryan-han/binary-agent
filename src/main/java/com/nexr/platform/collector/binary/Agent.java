package com.nexr.platform.collector.binary;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.nexr.platform.collector.util.ExponentialBackoff;

public class Agent {
	private static Logger log = Logger.getLogger(Agent.class);

	BinaryConfiguration conf = BinaryConfiguration.get();
	FileSystem hdfs;
	Path hdfsPath;
	// hdfs dir list
	private List<String> targetList = new CopyOnWriteArrayList<String>();
	// source path
	private File sourcePath;
	// source file list
	private List<String> sourceList = new CopyOnWriteArrayList<String>();

	SourceCheckThread sourceCheckThread;
	TargetCheckThread targetCheckThread;

	boolean checkTargetSuccess = false;
	ExponentialBackoff checkTargetBackoff = new ExponentialBackoff(500, 5);

	boolean copySuccess = false;
	ExponentialBackoff copyBackoff = new ExponentialBackoff(500, 5);

	public static void main(String args[]) {
		Agent agent = new Agent();

		if (!agent.checkConfiguration()) {
			System.exit(0);
		}

		agent.init();
		agent.start();

	}

	private void init() {
		try {
			hdfs = FileSystem.get(URI.create(conf.getAgentHdfsPath()), conf);
			hdfsPath = new Path(conf.getAgentHdfsPath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void start() {
		// TODO Auto-generated method stub
		sourcePath = new File(conf.getAgentSourcePath());
		if (!sourcePath.exists()) {
			log.info("Directory " + sourcePath + " not exist");
			sourcePath.mkdir();
		}

		sourceCheckThread = new SourceCheckThread();
		sourceCheckThread.start();

		targetCheckThread = new TargetCheckThread();
		targetCheckThread.start();

		CopyThread copyThread = new CopyThread();
		copyThread.start();

	}

	private void rename(String source) {
		File file = new File(source + ".FIN");
		file.renameTo(new File(source + ".FIN" + ".copyed"));

		file = new File(source);
		file.renameTo(new File(source + ".copyed"));
	}

	private boolean checkConfiguration() {
		if (conf.getAgentSourcePath() == null || conf.getAgentHdfsPath() == null) {
			log.error("Configuration is not complete !!");
			return false;
		}
		return true;
	}

	public void checkSource() {
		log.debug("check Source " + sourcePath);
		File[] fileList = sourcePath.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				log.debug("Searched Source " + pathname.getName());
				// TODO Auto-generated method stub
				if (conf.getAgentSourceSuffix() == null) {
					return true;
				} else {
					if (pathname.getAbsolutePath().endsWith(conf.getAgentSourceSuffix())) {
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

	public void checkTarget() {
		int retries = 0;
		while (!checkTargetSuccess) {
			try {
				log.debug("DFS Root " + hdfsPath.toString());
				if (hdfs.exists(hdfsPath)) {
					FileStatus[] stats = hdfs.listStatus(hdfsPath);
					Path[] listPaths = FileUtil.stat2Paths(stats);
					for (Path path : listPaths) {
						if (!targetList.contains(path.toString())) {
							targetList.add(path.toString());
							log.info("New Target created " + path.toString());
						}
					}
				} else {
					log.warn("DFS Root " + hdfsPath.toString() + " is not exist ");
				}
				checkTargetSuccess = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				long waitTime = checkTargetBackoff.sleepIncrement();
				log.info("attempt " + retries + " failed, backoff (" + waitTime
						+ "ms): " + e.getMessage());

				checkTargetBackoff.backoff();

				try {
					checkTargetBackoff.waitUntilRetryOk();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				retries++;
			}
			if (checkTargetSuccess) {
				checkTargetBackoff.reset();
				checkTargetSuccess = false;
				break;
			}
			if (checkTargetBackoff.isFailed()) {
				// 시스템 종
				log.error("System have a problem so shutdown !");
				System.exit(0);
			}
		}
	}

	public void copyFile(String source, String target) {
		int retries = 0;
		long startTime = System.currentTimeMillis();
		while (!copySuccess) {
			InputStream in = null;
			OutputStream out = null;
			try {
				in = new BufferedInputStream(new FileInputStream(new File(source)));

				Path targetPath = new Path(target);
				out = hdfs.create(targetPath);
				IOUtils.copyBytes(in, out, 4906, true);

				Path targetPathFIN = new Path(target + ".FIN");
				hdfs.createNewFile(targetPathFIN);

				log.info("Copy " + source + " To " + target);
				rename(source);
				copySuccess = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				long waitTime = copyBackoff.sleepIncrement();
				log.info("attempt " + retries + " failed, backoff (" + waitTime
						+ "ms): " + e.getMessage());
				e.printStackTrace();

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
		log.info("Total Time " + (System.currentTimeMillis() - startTime));
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
					Thread.sleep(conf.getSourceCheckTime());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	class TargetCheckThread extends Thread {

		public TargetCheckThread() {
			// TODO Auto-generated constructor stub
			super("TargetCheckThread");
		}

		public void run() {
			while (true) {
				checkTarget();
				try {
					Thread.sleep(conf.getHdfsDirCheckPeriod());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
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
				if (sourceList.size() > 0 && targetList.size() > 0) {
					for (String source : sourceList) {
						int index = idxKey % targetList.size();

						String t = targetList.get(index);
						// a.FIN이 만들어 지면 a라는 파일을 전송한다.
						String s = source.substring(0, source.length() - 4);
						File bs = new File(s);
						if (bs.exists()) {
							copyFile(s,
									t + source.substring(source.lastIndexOf("/"), s.length()));

							sourceList.remove(0);
							idxKey++;
						}
					}
				}

				try {
					Thread.sleep(conf.getCopyPeriid());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
