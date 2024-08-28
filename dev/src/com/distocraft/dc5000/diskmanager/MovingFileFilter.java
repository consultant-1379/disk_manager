package com.distocraft.dc5000.diskmanager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * File filter implementation that moves files.
 * 
 * Copyright Ab Ericsson Oy 2007
 * 
 * @author etuolem
 */
class MovingFileFilter extends DiskManagerFileFilter {

  private static final int BUFFER_SIZE = 16384;

  private Logger log;

  private File targetDir;

  MovingFileFilter(Properties conf, Logger log) throws Exception {
    super(conf);

    this.log = log;

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

  }

  public boolean accept(File f) {
    
    if (f.isDirectory()){
      return true;
	}
    
    try {

      if (ageCheck(f) > 0){
        move(f, targetDir);
	  }

    } catch (Exception e) {
      log.log(Level.INFO, "Filter failed for " + f, e);
    }

    return false;

  }

  private void move(File src, File tgtDir) throws Exception {

    File tgt = new File(tgtDir, src.getName());

    boolean success = src.renameTo(tgt);

    if (success){
      return;
	}
    else{
      log.finest("Moving file via file.renameTo failed");
	}

    InputStream in = new FileInputStream(src);
    OutputStream out = new FileOutputStream(tgt);

    byte[] buf = new byte[BUFFER_SIZE];
    int len;
    while ((len = in.read(buf)) > 0) {
      bytes += len;
      out.write(buf, 0, len);
    }
    in.close();
    out.close();

    src.delete();

    log.finest("File successfully moved via copy & delete");

  }

}
