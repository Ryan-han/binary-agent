package com.nexr.platform.collector.binary;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.nexr.platform.collector.util.ExponentialBackoff;

public class Collector {

	private static Logger log = Logger.getLogger(Collector.class);
	BinaryConfiguration conf = BinaryConfiguration.get();
	FileSystem hdfs;
	Path sourceHdfsPath;

	private List<String> sourceList = new CopyOnWriteArrayList<String>();
	// source path
	private File targetPath;

	boolean checkSourceSuccess = false;
	ExponentialBackoff checkSourceBackoff = new ExponentialBackoff(500, 5);

	boolean copySuccess = false;
	ExponentialBackoff copyBackoff = new ExponentialBackoff(500, 5);

	public static void main(String args[]) {

		Collector collector = new Collector();

		if (!collector.checkConfiguration()) {
			System.exit(0);
		}

		collector.init();
		collector.start();

	}

	private void start() {
		// TODO Auto-generated method stub
		SourceCheckThread targetCheckThread = new SourceCheckThread();
		targetCheckThread.start();

		CopyThread copyThread = new CopyThread();
		copyThread.start();
	}

	public void checkSource() {
		int retries = 0;
		while (!checkSourceSuccess) {
			try {
				log.debug("DFS Root " + sourceHdfsPath.toString());
				if (hdfs.exists(sourceHdfsPath)) {
					FileStatus[] stats = hdfs.listStatus(sourceHdfsPath);

					for (FileStatus state : stats) {

						Path path = state.getPath();
						if (!sourceList.contains(path.toString())
								&& path.getName().endsWith("FIN")) {
							// FIN이 있는지 check해서 있으면 FIN이 없는 파일을 add함.
							String source = path.toString().substring(0,
									path.toString().lastIndexOf("."));
							sourceList.add(source);
							log.info("New file added " + source);
						}
					}
				} else {
					log.warn("DFS Root " + sourceHdfsPath.toString() + " is not exist ");
				}
				checkSourceSuccess = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				long waitTime = checkSourceBackoff.sleepIncrement();
				log.info("attempt " + retries + " failed, backoff (" + waitTime
						+ "ms): " + e.getMessage());

				checkSourceBackoff.backoff();

				try {
					checkSourceBackoff.waitUntilRetryOk();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				retries++;
			}

			if (checkSourceSuccess) {
				checkSourceBackoff.reset();
				checkSourceSuccess = false;
				break;
			}
			if (checkSourceBackoff.isFailed()) {
				// 시스템 종
				log.error("System have a problem so shutdown !");
				System.exit(0);
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
					Thread.sleep(conf.getHdfsDirCheckPeriod());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private void init() {
		// TODO Auto-generated method stub
		try {
			hdfs = FileSystem.get(URI.create(conf.getCollectorHdfsPath()), conf);
			sourceHdfsPath = new Path(conf.getCollectorHdfsPath());

			if (!hdfs.exists(sourceHdfsPath)) {
				hdfs.mkdirs(sourceHdfsPath);
				log.info("Create Source Path " + sourceHdfsPath);
			}

			targetPath = new File(conf.getCollectorTargetPath());
			if (!targetPath.exists()) {
				targetPath.mkdir();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private boolean checkConfiguration() {
		// TODO Auto-generated method stub
		if (conf.getCollectorHdfsPath() == null
				|| conf.getCollectorTargetPath() == null) {
			log.error("Configuration is not complete !!");
			return false;
		}
		return true;
	}

	class CopyThread extends Thread {

		public CopyThread() {
			// TODO Auto-generated constructor stub
			super("CopyThread");
		}

		public void run() {
			while (true) {
				if (sourceList.size() > 0) {
					for (String source : sourceList) {
						Path sourcePath = new Path(source);
						try {
							if (hdfs.exists(new Path(source))) {
								copyFile(
										sourcePath,
										targetPath
												+ source.substring(source.lastIndexOf("/"),
														source.length()));

								sourceList.remove(0);
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
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

	public synchronized void copyFile(Path source, String target) {
		int retries = 0;
		long startTime = System.currentTimeMillis();
		while (!copySuccess) {
			InputStream in = null;
			OutputStream out = null;
			try {

				in = hdfs.open(source);
				out = new FileOutputStream(new File(target));

				IOUtils.copyBytes(in, out, 4906, true);
				File targetFin = new File(target+".FIN");
				targetFin.createNewFile();
				
				log.info(hdfs.exists(source) + " Copy " + source + " To " + target);
				rename(source);

				copySuccess = true;
			} catch (IOException e) {
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
		log.info("Total Time " + (System.currentTimeMillis() - startTime));
	}

	private void rename(Path source) {
		Calendar c = Calendar.getInstance();
		Path backup = new Path(conf.getCollectorBackupPath() + "/"
				+ source.getParent().getName() + "/" + c.get(Calendar.YEAR) + "-"
				+ (c.get(Calendar.MONTH) + 1) + "-" + c.get(Calendar.DATE) + "-"
				+ c.get(Calendar.HOUR));

		try {
			if (!hdfs.exists(backup)) {
				hdfs.mkdirs(backup);

			}

			hdfs.rename(source, backup);
			hdfs.delete(source);
			hdfs.delete(new Path(source.toString() + ".FIN"));

			log.info("Moved " + source + " to " + backup.toString() + " Deleted "
					+ source + ", " + source.toString() + ".FIN");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
