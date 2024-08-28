package com.distocraft.dc5000.diskmanager;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.distocraft.dc5000.common.StaticProperties;

class ZippingFileFilter extends DiskManagerFileFilter {

  private static final int BUFFER_SIZE = 16384;

  public static final int MAX_ZIP_FILES = 60000;

  private Logger log;

  private int maxFilesToArchive;

  protected File targetDir = null;
  
  protected File sourceDir = null;

  private int zipFileCount = 0;

  private String archivePrefix;

  private int archiveMode;

  private SimpleDateFormat outputFormat;

  private File outFile = null;

  private ZipOutputStream out = null;

  private int fileCount = 0;

  private long minDate = Long.MAX_VALUE;

  private long maxDate = Long.MIN_VALUE;
  
  // Log files added to this array will be copied to a new file. The newly created file will then be archived and deleted.
  private String[] logsToCopyThenZip = {"catalina.out"};

  ZippingFileFilter(Properties conf, Logger log) throws Exception {
    super(conf);

    this.log = log;

    this.maxFilesToArchive = Integer.parseInt(StaticProperties.getProperty("diskManager.maxFilesToArchive", String
        .valueOf(MAX_ZIP_FILES)));

    if (this.maxFilesToArchive < 1){
      this.maxFilesToArchive = MAX_ZIP_FILES;
    }
    if (this.maxFilesToArchive > MAX_ZIP_FILES){
      this.maxFilesToArchive = MAX_ZIP_FILES;
    }
    
    String directory = conf.getProperty("diskManager.dir.outDir");

    if (directory.indexOf("${") >= 0) {
      int start = directory.indexOf("${");
      int end = directory.indexOf("}", start);

      if (end >= 0) {
        String variable = directory.substring(start + 2, end);
        String val = System.getProperty(variable);
        String result = directory.substring(0, start) + val + directory.substring(end + 1);
        directory = result;
      }
    }

    targetDir = new File(directory);

    if (!targetDir.isDirectory()){
      throw new Exception("Target directory \"" + targetDir + "\" does not exist or is not a directory.");
    }
    if (!targetDir.canWrite()){
      throw new Exception("Cannot write to target directory \"" + targetDir + "\"");
    }
    
    String srcDirectory = conf.getProperty("diskManager.dir.inDir");

    if (srcDirectory.indexOf("${") >= 0) {
      int start = srcDirectory.indexOf("${");
      int end = srcDirectory.indexOf("}", start);

      if (end >= 0) {
        String variable = srcDirectory.substring(start + 2, end);
        String val = System.getProperty(variable);
        String result = srcDirectory.substring(0, start) + val + srcDirectory.substring(end + 1);
        srcDirectory = result;
      }
    }

    sourceDir = new File(srcDirectory);

    if (!sourceDir.isDirectory()){
      throw new Exception("Target directory \"" + sourceDir + "\" does not exist or is not a directory.");
    }
    if (!sourceDir.canWrite()){
      throw new Exception("Cannot write to target directory \"" + sourceDir + "\"");
    }
    
    archiveMode = Integer.parseInt(conf.getProperty("diskManager.dir.archiveMode"));

    archivePrefix = conf.getProperty("diskManager.dir.archivePrefix");

    if (archivePrefix == null || archivePrefix.length() <= 0){
      throw new Exception("Parameter archivePrefix is invalid");
	}

    try {
      outputFormat = new SimpleDateFormat(conf.getProperty("diskManager.dir.dateFormatOutput", "yyyyddMMhhmmss"));
    } catch (Exception e) {
      throw new Exception("Paramter dateFormatOuput is invalid", e);
    }

  }

  public boolean accept(File f) {

    if (f.isDirectory()){
      return true;
	}

    BufferedInputStream bi = null;

    try {

      long filemod = ageCheck(f);

      if (filemod < 0){
        return false;
	  }
      
      String now = outputFormat.format(new Date());
      
      for(String logFile : logsToCopyThenZip) {
	      if(f.getName().equalsIgnoreCase(logFile)) {
	    	  
	    	  // Coping the original file to a new file with time stamp. 
	    	  // This will ensure that the original file is nor zipped or deleted. 
	    	  File copy = new File(sourceDir, f.getName() + "." + now);
	    	  Files.copy(f.toPath(), copy.toPath());
	    	  
	    	  f = copy;
	    	  break;	    		  
	      }
      }

      if (fileCount >= maxFilesToArchive || out == null) {

        String name = "diskmanager_tmp_" + now + ".zip";

        outFile = new File(targetDir, name);

        log.fine("Opening Zip file: " + outFile);

        out = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)));

        zipFileCount++;

      }

      byte data[] = new byte[BUFFER_SIZE];

      bi = new BufferedInputStream(new FileInputStream(f), BUFFER_SIZE);

      ZipEntry entry = new ZipEntry(f.getName());
      if (out == null){
    	  out = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)));
      }
      out.putNextEntry(entry);
      int count;

      while ((count = bi.read(data, 0, BUFFER_SIZE)) != -1) {
        bytes += count;
        out.write(data, 0, count);
      }

      log.finest("File " + f + " successfully added into zip");

      if (filemod > maxDate){
        maxDate = filemod;
	  }

      if (filemod < minDate){
        minDate = filemod;
      }

      boolean succ = f.delete();
      if (!succ){
        log.warning("Unable delete file " + f);
	  }
      else{
        log.finest("File " + f + " deleted");
	  }

    } catch (Exception e) {
      log.log(Level.INFO, "Filter failed for " + f, e);
    } finally {
      if (f != null) {
        try {
          bi.close();
        } catch (Exception e) {
        }
      }
    }

    return false;
  }

  void close() {

    if (out != null) {
      try {
        out.close();
      } catch (Exception e) {
      }
    }

    if (null != outFile) {
      try {

        String targetFilename = "error";

        if (archiveMode == DiskManager.ZIP_CREATIONTIME) {

          targetFilename = archivePrefix + outputFormat.format(new Date(minDate)) + "-"
              + outputFormat.format(new Date(maxDate)) + ".zip";

        } else if (archiveMode == DiskManager.ZIP_MINMAX) {

          targetFilename = archivePrefix + outputFormat.format(new Date()) + ".zip";

        } else if (archiveMode == DiskManager.ZIP_SEQUENCE) {

          targetFilename = archivePrefix + zipFileCount + ".zip";

        }

        File tfile = new File(targetDir, targetFilename);

        if (tfile.exists()){
          tfile.delete();
		}

        outFile.renameTo(tfile);
        out=null;
      } catch (Exception e) {
        log.log(Level.WARNING, "Error while renaming zip file", e);
      }
    }
  }

}
