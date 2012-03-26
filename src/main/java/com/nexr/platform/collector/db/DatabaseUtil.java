package com.nexr.platform.collector.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.nexr.platform.collector.sftp.AgentInfo;

public class DatabaseUtil {
	static Logger log = Logger.getLogger(DatabaseUtil.class);

	Connection con = null;
	Statement stmt = null;
	
	String url;
	String user;
	String password;
	String listTable;
	String sentTable;
	
	public DatabaseUtil(String url, String user, String password, String listTable, String sentTable) {
		this.url = url;
		this.user = user;
		this.password = password;
		this.listTable = listTable;
		this.sentTable = sentTable; 
	}
	
	

	private Connection getConnection() {
		if (con == null) {
			try {
				Class.forName("com.mysql.jdbc.Driver");
				con = DriverManager.getConnection(url,
						user, password);
			} catch (java.lang.ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO: handle exception
				e.printStackTrace();
			}

			return con;
		} else {
			return con;
		}
	}

	public boolean dropTable() {
		boolean result = false;
		String dropString = "DROP TABLE " + listTable;

		try {
			stmt = getConnection().createStatement();
			stmt.executeUpdate(dropString);
			stmt.close();
			result = true;
		} catch (SQLException ex) {
			con = null;
			ex.printStackTrace();
		}

		log.info("Drop " + listTable + " Success ");
		return result;
	}

	public boolean createListTable() {
		boolean result = false;
		String createString = "CREATE TABLE " + listTable + " ( fileName VARCHAR(100), tot_cnt INTEGER )";

		try {
			stmt = getConnection().createStatement();
			stmt.executeUpdate(createString);

			stmt.close();
			result = true;
		} catch (SQLException ex) {
			ex.printStackTrace();

		}
		log.info("Create " + listTable + " Success ");
		return result;
	}

	public boolean uploadListToDB(File listFile) {
		boolean result = false;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					listFile)));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String line = null;

		if (listFile.exists()) {
			try {
				while ((line = br.readLine()) != null) {
					String[] fields = line.split("\t");
					String insert = "insert into " + listTable + " values ('" + fields[0].trim()
							+ "'," + fields[1].trim() + ")";
					try {
						stmt = getConnection().createStatement();
						stmt.executeUpdate(insert);
						stmt.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						con = null;
						e.printStackTrace();
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return result;
	}

	public boolean insertSentToDB(String file) {
		boolean result = false;

		log.info("FileName " + file);
		String insert = "insert into " + sentTable + " values ('" + file + "')";
		log.info(insert);
		try {
			stmt = getConnection().createStatement();
			stmt.executeUpdate(insert);
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			con = null;
			e.printStackTrace();
		}

		return result;
	}

	public List<String> getRetrieveFiles() {
		List<String> files = new ArrayList<String>();
		String selectQuery = "select filename from " + listTable + " where filename NOT IN (Select filename from " + sentTable + ")";
		try {
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(selectQuery);
			while (rs.next()) {
				String fileName = rs.getString("filename");
				log.info(fileName);
				files.add(fileName);
			}
			stmt.close();
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			con = null;
			e.printStackTrace();
		}
		return files;
	}

	private boolean importListToDB(String listFile) {
		boolean result = false;

		// String query = "LOAD DATA INFILE '" + listFile
		// + "' INTO TABLE CDR_LIST (fileName, tot_cnt)";
		String query = "LOAD DATA INFILE '"
				+ listFile
				+ "' INTO TABLE CDR_LIST FIELDS TERMINATED BY '/t' LINES TERMINATED BY '\n' (fileName, tot_cnt)";

		try {
			stmt = getConnection().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			stmt.executeUpdate(query);
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			con = null;
			e.printStackTrace();
		}
		return result;
	}

}
