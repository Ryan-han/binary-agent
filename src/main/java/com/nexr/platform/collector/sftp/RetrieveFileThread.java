package com.nexr.platform.collector.sftp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

import com.nexr.platform.collector.db.DatabaseUtil;

/* 
 * LIST파일이 오면 파일의 파일명을 DB 에insert하
 * 보낸 데이터와 비교하여 보내지 않은 파일만 다시 FTP로 가져온다.
 * 
 * 데이타를 가져온 후에는 LIST의 데이타를 DB에서 모두 지운다.
 * 
 */

public class RetrieveFileThread extends Thread {

	static Logger log = Logger.getLogger(RetrieveFileThread.class);

	// List<String> files;
	AgentInfo sourceInfo;
	FTPClient ftp;
	String targetDir;
	String listFile;
	DatabaseUtil dbUtil;
	String datExt;
	String finExt;

	private boolean isFinish = false;
	
	public RetrieveFileThread(AgentInfo sourceInfo, String targetDir,
			DatabaseUtil dbUtil, String listFile, String datExt, String finExt) {
		this.sourceInfo = sourceInfo;
		this.targetDir = targetDir;

		ftp = new FTPClient();

		try {
			ftp.setControlEncoding("UTF-8");
			ftp.connect(sourceInfo.getHost());
			ftp.login(sourceInfo.getUser(), sourceInfo.getPasswd());
			ftp.enterLocalPassiveMode();
			ftp.changeWorkingDirectory(sourceInfo.getTargetPath());
			ftp.setFileType(FTP.BINARY_FILE_TYPE);

			log.info("FTP Connection Success");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		this.dbUtil = dbUtil;
		this.listFile = listFile;
		this.finExt = finExt;
		this.datExt = datExt;

		log.info("List filename : " + listFile);

	}

	public void run() {
		log.info("start Retrieve File");
		// importListToDB("/Users/ryan/tmp/test.LST");

		dbUtil.dropTable();

		dbUtil.createListTable();

		dbUtil.uploadListToDB(new File(listFile));

		List<String> files = dbUtil.getRetrieveFiles();

		retrieveFile(files);

		File listf = new File(listFile);
		listf.delete();
		isFinish = true;

	}
	
	public boolean isFinish() {
		return isFinish;
	}
	
	private void retrieveFile(List<String> files) {
		int matchCount = 0;
		OutputStream outputStream = null;
		try {
			FTPFile[] ftpFiles = ftp.listFiles();
			for (String file : files) {
				for (FTPFile ftpFile : ftpFiles) {
					if (file.equals(ftpFile.getName())) {
						if (outputStream != null) {
							outputStream = null;
						}
						log.info("file : " + targetDir + "/" + file + ", ftpFile : "
								+ ftpFile.getName());
						outputStream = new FileOutputStream(targetDir + "/"
								+ ftpFile.getName());
						ftp.retrieveFile(ftpFile.getName(), outputStream);
						String finFile = targetDir + "/" + file.replace(datExt, finExt);
						File fin = new File(finFile);
						fin.createNewFile();
						matchCount++;
						outputStream.close();
					}

				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.info("Finish Retrieve : List = " + files.size() + " Retrieve = "
				+ matchCount);
	}

}
