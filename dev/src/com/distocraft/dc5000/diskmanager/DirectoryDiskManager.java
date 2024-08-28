package com.distocraft.dc5000.diskmanager;

/**
 * Directory DiskManager archives directories to zip files. This action is designed for usage
 * with parser archive-directories only.
 * 
 * Directory DiskManager is executed via specific ETLC action <br>
 * <br>
 * <table border="1" width="100%" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#CCCCFF" class="TableHeasingColor">
 * <td colspan="4"><font size="+2"><b>Parameter Summary</b></font></td>
 * </tr>
 * <tr>
 * <td><b>Name</b></td>
 * <td><b>Key</b></td>
 * <td><b>Description</b></td>
 * <td><b>Default</b></td>
 * </tr>
 * <tr>
 * <td>IN Directory</td>
 * <td>inDir</td>
 * <td>Input directory. Where archived files/directories are read.</td>
 * <td>Default</td>
 * </tr>
 * <tr>
 * <td>OUT Directory</td>
 * <td>outDir</td>
 * <td>Output directory. Where created archives are written.</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>Dir age in hours</td>
 * <td>timeLimit</td>
 * <td>Timelimit in hours. Older directories are archived.</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>Archive prefix</td>
 * <td>prefix</td>
 * <td>Prefix used to name archive files.</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>deleteDirectory</td>
 * <td>Are directories deleted after archive creation.</td>
 * <td>true</td>
 * </tr>
 * <tr>
 * <td></td>
 * <td>maxEntriesPerZip</td>
 * <td>Maximum number of files inserted per archive. If there are more files than maxEntriesPerZip extra numbered archives are created.</td>
 * <td>50000</td>
 * </tr>
 * </table>
 * <br> 
 *
 * @author melantie
 * Copyright Distocraft 2006
 */

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class DirectoryDiskManager {

  private static final int ZIP_BUFFER = 16384;

  int timeLimit = 2;

  boolean deleteDirectory = true;

  int maxEntriesPerZip = 50000;

  private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");

  // COMMON PARAMETERS
  private static Logger log;

  private Properties conf;

  private File in;

  private File out;

  private String prefix;

  /**
   * Initializes DirectoryDiskManager for defined instance.
   * 
   * @param instanceName
   *          name of instance in config-file.
   * @param techPack
   *          Technology package name
   * @param set_type
   *          Name of the set type
   * @param set_name
   *          Name of the set
   */
  public DirectoryDiskManager(Properties conf, String techPack, String set_type, String set_name) throws Exception {

    this.conf = conf;

    log = Logger.getLogger("etl." + techPack + "." + set_type + "." + set_name + ".directoryDiskManager");

    this.prefix = conf.getProperty("directoryDiskManager.prefix", "archive");

    in = new File(conf.getProperty("directoryDiskManager.inDir"));
    if (!in.exists() || !in.isDirectory()){
      throw new Exception("Check in directory " + in);
	}
    out = new File(conf.getProperty("directoryDiskManager.outDir"));
    if (!out.exists() || !out.isDirectory()){
      throw new Exception("Check out directory " + out);
	}

    this.timeLimit = toInt(conf.getProperty("directoryDiskManager.timeLimit"));
    this.deleteDirectory = "true".equalsIgnoreCase(conf.getProperty("directoryDiskManager.deleteDirectory", "true"));
    this.maxEntriesPerZip = toInt(conf.getProperty("directoryDiskManager.maxEntriesPerZip", "50000"));
  }

  /**
   * List directories to archive Archive (zip) Delete archived directories
   * 
   * @throws Exception
   */
  public void execute() throws Exception {

    log.finest("Executing...");
    log.finest("Prefix: " + prefix);
    log.finest("In directory: " + in);
    log.finest("Out directory: " + out);
    log.finest("Timelimit: " + timeLimit);
    log.finest("Delete directory: " + deleteDirectory);

    long starttime = System.currentTimeMillis();

    File[] dirToZip = subDirectories(in);

    for (int i = 0; i < dirToZip.length; i++) {
      log.info("Archiving directory " + dirToZip[i]);
      
      zipDirectory(dirToZip[i]);

      if (deleteDirectory){
        delDirectory(dirToZip[i]);
	  }
    }

    log.fine("Directory diskmanager finnished.");
    long endtime = (System.currentTimeMillis());
    log.finest("Execution time in seconds: " + new Date(endtime - starttime).getSeconds());
  }

  /**
   * Zip the contents of the directory, and save it in the zipfile
   * 
   * @param inputdir
   * @throws IllegalArgumentException
   */
  public void zipDirectory(File inputdir) throws Exception, IllegalArgumentException {

    // Check that the directory is a directory, and get its contents
    if (!inputdir.isDirectory()){
      throw new IllegalArgumentException("Not a directory:  " + inputdir);
	}

    File[] entries = inputdir.listFiles();
    log.finest("File list length: " + entries.length);

    int zippedEntries = 0;
    byte[] buffer = new byte[ZIP_BUFFER]; // Create a buffer for copying
    int bytesRead;
    int count = 0;

    // there should be atleast one entry in directory
    if (entries.length > 0) {

      // if entrie count is larger than maxEntriesPerZip
      while ((entries.length - zippedEntries) > 0) {

        File zout;
        if (count == 0){
          zout = new File(out, prefix + inputdir.getName() + ".zip");
		}
        else{
          zout = new File(out, prefix + inputdir.getName() + "_" + count + ".zip");
		}

        // create zipfile
        log.info("Outputfile is: " + zout);
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zout));
        out.setLevel(1); // fastest

        try {

          int zip = zippedEntries + maxEntriesPerZip;
          if (zip > entries.length){
            zip = entries.length;
		  }

          for (int i = zippedEntries; i < zip; i++) {
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(entries[i]), ZIP_BUFFER);

            ZipEntry entry = new ZipEntry(entries[i].getPath()); // Make a
            // ZipEntry
            log.fine(i + " Adding file: " + entries[i]);
            out.putNextEntry(entry); // Store entry

            while ((bytesRead = in.read(buffer)) != -1){
              out.write(buffer, 0, bytesRead);
			}
            in.close();
          }

        } catch (Exception e) {
          log.log(Level.WARNING, "ZipDirectory Exception", e);
        } finally {
          out.finish();
          out.close();
        }

        zippedEntries += maxEntriesPerZip;
        count++;
      }

    }
  }

  /**
   * Deletes directory.
   * 
   * @param d
   * @throws Exception
   * @throws IllegalArgumentException
   */
  public void delDirectory(File d) throws Exception, IllegalArgumentException {

    // Check that the directory is a directory, and get its contents
    if (!d.isDirectory()){
      throw new IllegalArgumentException("Not a directory:  " + d);
	}

    File[] entries = d.listFiles();
    log.info("File list length: " + entries.length);

    long endTime = System.currentTimeMillis();

    log.info("Delete directory: " + d.getAbsolutePath() + "\\");

    boolean success = deleteDir(d, entries);

    log.info("Deletion: " + success);

    long deleted = System.currentTimeMillis();

    log.info("Deletion took " + (deleted - endTime) + " ms");

  }

  /**
   * Deletes all files and subdirectories under dir. Returns true if all
   * deletions were successful. If a deletion fails, the method stops attempting
   * to delete and returns false.
   */
  public static boolean deleteDir(File dir, File[] entries) {
    if (dir.isDirectory()) {
      for (int i = 0; i < entries.length; i++) {
        log.fine("Deleting file : " + entries[i].getAbsolutePath());
        boolean success = entries[i].delete();
        if (!success) {
          return false;
        }
      }
    }
    // The directory is now empty so delete it
    return dir.delete();
  }

  /**
   * List all subdirectories, excluded directories which starts with _ and
   * directories which are not older than timelimit
   * 
   * @param directory
   * @return
   */

  public File[] subDirectories(File dir) throws Exception {

    FilenameFilter filter = new FilenameFilter() {

      public boolean accept(File d, String name) {

        // Temporary directories starts with _ -> ignore those
        if (name.startsWith("_")){
          return false;
		}

        // Filename contains no _ -> ignore
        if (name.indexOf("_") < 1){
          return false;
		}

        // The file is actually a file -> ignore
        File f = new File(d, name);
        if (f.isFile()){
          return false;
		}

        try {

          String[] tmp = name.split("_");
          log.fine("Folder timestamp: " + tmp[1]);
          Date date = sdf.parse(tmp[1]);
          long folderTime = date.getTime();
          long currentTime = System.currentTimeMillis();

          long diff = (currentTime - folderTime) / 3600000;

          if (diff > timeLimit){
            return true;
		  }
          else{
            return false;
		  }

        } catch (ParseException pe) {
          log.info("Not filename: " + name);
          return false;
        }

      }
    };

    return dir.listFiles(filter);

  }

  /**
   * Converts string to int
   */
  private static int toInt(String str) throws ParseException {
    if (str != null){
      return Integer.valueOf(str.trim()).intValue();
	}
    else{
      log.finest("Timelimit property not found. Using 0.");
	}
    return 0;
  }

}
