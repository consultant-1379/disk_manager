package com.distocraft.dc5000.diskmanager;

import java.io.File;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * File filter implementation that deletes files.
 * 
 * Copyright Ab Ericsson Oy 2007
 * 
 * @author etuolem
 */
public class DeletingFileFilter extends DiskManagerFileFilter {

  Logger log;

  DeletingFileFilter(Properties conf, Logger log) throws Exception {
    super(conf);

    this.log = log;
  }


  @Override
  public boolean accept(File f) {

    String dirPath = f.getAbsolutePath();
    //Check if deleting files in failed dir of etldata..
    if ( (dirPath.contains("etldata")) && (dirPath.contains("failed"))){
      log.finest("Checking accept rules for file: " + dirPath);
      try {
    	//Code changes for TR HQ93149
		if ((!f.isDirectory() && f.getAbsolutePath().endsWith("zip")) || f.getAbsolutePath().endsWith("xml")) {
          long age = ageCheck(f);

          if (age > 0) {
            log.fine("File "+dirPath+" is over 60 days old!!");
            boolean suc = f.delete();
            if(suc){
              log.fine("File successfully deleted in failed directory : "+dirPath);
            }
            return true;

          } else {
            log.fine(" File " + dirPath
                + " is not old enough for deletion");
            }
        } else {
          log.finest(" "+dirPath + " is a directory or not a zip file!!");
          return true;
        }
      } catch (Exception e) {
        log.info(" Exception while deleting files in etldata directory." +e.getMessage());


      }
      log.finest(" Skipped failed block.. returning false. ");
      return false;
    }else{

      if (f.isDirectory()) {
        return true;
      }
      try {

        if(super.ageCheck(f) > 0) {
          if(!f.getName().equals(".tagfile")) { // HP49595, diskmanager should skip .tagfile(used by NAS)deletion
            boolean suc = f.delete(); 
            if(!suc){
              log.info("Cannot delete file " + f);
			}
          }
            }

      } catch(Exception e) { 
          log.log(Level.INFO, "Filter failed for " + f,e); }



      return false;

    }
  }

  /**
   * 
   * Overriding DiskMangerFileFilter's ageCheck()

   * @param file
   * @param conf
   * @return
   * @throws Exception
   * 
   * 
   */


  @Override
  long ageCheck(File file) throws Exception {
    
    long start = System.currentTimeMillis();
    totalfiles++;
    log.finest("FileAge for file "+file+" is :" + fileAge);
    long filemodtime = -1;

    log.finest("Name of file to be deleted is :" + file.getName());
    filemodtime = file.lastModified();
    log.finest("filemodtime for file to be deleted is : " + filemodtime);
    long time = start - filemodtime;
    log.finest("starttime for file to be deleted is: " + start);
    if (filemodtime==0){
        // 0L if the file does not exist (if a symbolic link exists but real files does not).
        // We want to remove the file in this case. 
        files++;
        return 1L;      
      } else if ((time) > fileAge) {
        files++;
        return filemodtime;
      } else {
        return -1;
      }
  }
}