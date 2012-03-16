package com.nexr.platform.collector.binary;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


public class BinaryConfiguration extends Configuration {

	private static Logger log = Logger.getLogger(BinaryConfiguration.class);

	private static BinaryConfiguration singleton;
	
	public synchronized static BinaryConfiguration get() {
    if (singleton == null)
      singleton = new BinaryConfiguration();
    return singleton;
  }
	protected BinaryConfiguration() {
		super();

		Path conf = new Path(getConfDir());
		log.info("Loading configurations from " + conf);
		super.addResource(new Path(conf, "agent-conf.xml"));
		super.addResource(new Path(conf, "agent-site.xml"));

	}

	public static String getConfDir() {
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
	
	
	public static final String AGENT_HDFS_PATH = "agent.dfs.path";
	public static final String AGENT_SOURCE_PATH = "agent.source.path";
	public static final String AGENT_SOURCE_SUFFIX = "agent.source.suffix";
	
	public static final String HDFS_DIR_CHECK_PERIOD = "dir.check.period";
	public static final String SOURCE_CHECK_TIME = "source.check.time";
	public static final String COPY_PERIOD = "copy.period";
	
	public static final String HDFS_BACKOFF_CEILING ="dfs.backoff.ceiling";
	public static final String HDFS_BACKOFF_MAX ="dfs.backoff.max";
	
	public static final String COLLECTOR_HDFS_PATH = "collector.dfs.path";
	public static final String COLLECTOR_TARGET_PATH= "collector.target.path";
	public static final String COLLECTOR_HDFS_BACKUPPATH= "collector.backup.path";
	
	
	public long getHdfsBackoffCeiling() {
    return getLong(HDFS_BACKOFF_CEILING, 1 * 1000); // millis
  }
	
	public long getHdfsBackoffMax() {
    return getLong(HDFS_BACKOFF_MAX, 1000); 
  }
	
	public String getAgentHdfsPath() {
    return get(AGENT_HDFS_PATH, null);
  }
	
	public String getAgentSourcePath() {
    return get(AGENT_SOURCE_PATH, null);
  }
	
	public String getAgentSourceSuffix() {
    return get(AGENT_SOURCE_SUFFIX, null);
  }
	
	public long getHdfsDirCheckPeriod() {
    return getLong(HDFS_DIR_CHECK_PERIOD, 60 * 1000); // millis
  }
	
	public long getSourceCheckTime() {
    return getLong(SOURCE_CHECK_TIME, 5 * 1000); // millis
  }
	
	public long getCopyPeriid() {
    return getLong(COPY_PERIOD, 1 * 1000); // millis
  }
	
	public String getCollectorHdfsPath() {
    return get(COLLECTOR_HDFS_PATH, null);
  }
	
	public String getCollectorTargetPath() {
    return get(COLLECTOR_TARGET_PATH, null);
  }
	
	public String getCollectorBackupPath() {
    return get(COLLECTOR_HDFS_BACKUPPATH, null);
  }

}
