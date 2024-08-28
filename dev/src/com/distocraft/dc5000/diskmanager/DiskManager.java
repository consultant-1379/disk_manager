package com.distocraft.dc5000.diskmanager;

import com.ericsson.eniq.common.CommonUtils;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DiskManager archives files from specified input directories. <br>
 * <br>
 * DiskManager is executed via specific ETLC action <br>
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
 * <td> Dircetory where managed files are read.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Archiving mode</td>
 * <td>archiveMode</td>
 * <td>What is done with the managed files.<br>
 * "Use file modiftime" moves the managed files to the output directory.<br>
 * "create archive named by archive prefix" creates an archive named as the
 * archive prefix.<br>
 * "create archive named by current day" Cretaes an archive named archive prefix +
 * current timestamp.<br>
 * "create archive named by youngest and oldest file" creates an arcive named
 * archive prefix + timestamp of the younges file in archive + "_" + oldest file
 * in archive.<br>
 * "delete files" Deletes files.</td>
 * <td>"move files to out dir"</td>
 * </tr>
 * <tr>
 * <td>OUT Directory</td>
 * <td>outDir</td>
 * <td>Dircetory where managed files or archives are writen.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>File age in days</td>
 * <td>fileAgeDay</td>
 * <td>Determines the mininum file age in days (24h) for managed files.</td>
 * <td>&nbsp;</td>
 * <tr>
 * <td>File age in hours</td>
 * <td>fileAgeHour</td>
 * <td>Determines minimum file age in hours for managed files. Actual age is
 * defined by summing (in hours) the file age in hours and file age in days.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Directory depth</td>
 * <td>directoryDepth</td>
 * <td>Depth of the recursion (how many subdirectories are searched) when
 * listing managed files.</td>
 * <td> 2 </td>
 * <tr>
 * <td>File mask</td>
 * <td>fileMask</td>
 * <td>RegExp mask to select managed files from IN Directory.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>fileListExt</td>
 * <td>Filename extension used in file lists. see fileList </td>
 * <td>.txt</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>preview</td>
 * <td>If preview is set to 1 no actual changes are made.</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>fileList</td>
 * <td>If fileList is set to 1 filelist (list of all files managed by
 * diskmanager) are generated. Name of the list is same as archives name except
 * the file extension is fileListExt </td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>maxFilesToArchive</td>
 * <td>How many files can be added to one zip-file. If there are more managed
 * files than maxFilesToArchive new (numbered) archive is created.</td>
 * <td>60000</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>locale</td>
 * <td>Defines the locale used (timestamp).</td>
 * <td>en</td>
 * </tr>
 * <tr>
 * <td>File age mode</td>
 * <td>fileAgeMode</td>
 * <td>Defines where the timestamp of managed file is retrieved.<br>
 * "Use file modiftime" reads the timestamp from fileage (last modified) of the
 * file.<br>
 * "parse timestamp from filename" reads the timestamp from the filename using
 * filename time mask. See filename time mask.</td>
 * <td>"Use file modiftime"</td>
 * </tr>
 * <tr>
 * <td>Filename time mask</td>
 * <td>timeMask</td>
 * <td>RegExp mask that defines the timestamp from filename.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Date format output</td>
 * <td>dateFormatInput</td>
 * <td>Defines the output format (simpledateformat) of the timestamp. In what
 * format timestamp is written in the archive (or filelist). Do not use minutes
 * (mm) or seconds (ss).</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Date format output</td>
 * <td>dateFormatOutput</td>
 * <td>Defines the input format (simpledateformat) of the timestamp. In what
 * format timestamp is expectod to be when read from filename. See. Filename
 * time mask.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Delete empty directories</td>
 * <td>deleteEmptyDirectories</td>
 * <td>Are empty directories removed after files have been
 * moved/archived/deleted.</td>
 * <td>false</td>
 * </tr>
 * </table><br>
 * <p/>
 * Copyright Ab LM Ericsson Oy 2005-7 <br>
 *
 * @author savinen
 * @author lemminkainen
 */
class DiskManager {

  public static final int MAX_FILES_PER_ZIP = 60000;

  public static final int ZIP_BUFFER = 16384;

  // Archiving modes

  public static final int NOT_ARCHIVED = 0;

  public static final int ZIP_SEQUENCE = 1;

  public static final int ZIP_MINMAX = 2;

  public static final int ZIP_CREATIONTIME = 3;

  public static final int DELETED = 4;

  // File age modes

  public static final int FILE_SYSTEM_AGE = 0;

  public static final int FILENAME_AGE = 1;

  public static final String EVENTS_ETLDATA_DIR = "EVENTS_ETLDATA_DIR";
  private static final String EVENTS_ETLDATA_DIR_VAL = "/eniq/data/etldata_";

  // Common

  private Properties conf;

  private Logger log;

  private boolean deleteEmptyDirs = false;

  private int maxDepth = 2;

  /**
   * Initializes DiskManager for defined instance.
   *
   * @param conf     configuration
   * @param techPack Technology package name
   * @param set_type Name of the set type
   * @param set_name Name of the set
   */
  DiskManager(final Properties conf, final String techPack, final String set_type,
              final String set_name) throws Exception {
    this.conf = conf;
    this.log = Logger.getLogger("etl." + techPack + "." + set_type + "." + set_name + ".diskManager");
  }

  /**
   * Moves and archives files according to configuration
   */
  void execute() throws Exception {

    long execstart = System.currentTimeMillis();

    final String amode = conf.getProperty("diskManager.dir.archiveMode");

    if (amode == null || amode.length() <= 0) {
      throw new Exception("Parameter archiveMode not defined");
    }
    final int archiveMode = Integer.parseInt(amode);

    String ti_directory = conf.getProperty("diskManager.dir.inDir", null);

    if (ti_directory == null || ti_directory.length() <= 0) {
      throw new Exception("Parameter inDir not specified.");
    }

    if (ti_directory.contains("${")) {
      int start = ti_directory.indexOf("${");
      int end = ti_directory.indexOf("}", start);

      if (end >= 0) {
        String variable = ti_directory.substring(start + 2, end);
        String val = System.getProperty(variable);
        ti_directory = ti_directory.substring(0, start) + val + ti_directory.substring(end + 1);
      }
    }

    final File inDir = new File(ti_directory);

    if (!inDir.exists() || !inDir.isDirectory() || !inDir.canRead()) {
      log.warning("Input directory " + inDir.getName() + " can't be read. Exiting.");
      return;
    }

    final String smaxd = conf.getProperty("diskManager.dir.directoryDepth", "2");
    if (smaxd == null || smaxd.length() <= 0) {
      throw new Exception("Parameter directoryDepth not defined");
    }

    maxDepth = Integer.parseInt(smaxd);

    if ("true".equalsIgnoreCase(conf.getProperty("diskManager.dir.deleteEmptyDirectories", "false"))) {
      deleteEmptyDirs = true;
    }

    // Execute parameters handled

    try {

      log.info("Managing directory " + inDir);

      if (archiveMode == NOT_ARCHIVED) {
        final MovingFileFilter mff = new MovingFileFilter(conf, log);
        recurseDirectory(inDir, 0, mff);

        log.info("Successfully managed " + mff.getFiles() + "/" + mff.getTotalFiles() + " files (" + mff.getBytes()
          + "B) in " + (System.currentTimeMillis() - execstart) + " ms");
      } else if (archiveMode == ZIP_SEQUENCE || archiveMode == ZIP_MINMAX || archiveMode == ZIP_CREATIONTIME) {

        final ZippingFileFilter zff;
        /**** Check if the delete is for failed dirs in $etldata/ ***/
        if (inDir.getAbsolutePath().contains("etldata")) {

          final int numMountPoints = CommonUtils.getNumOfDirectories(log);
          zff = new ZippingFileFilter(conf, log);
          if (numMountPoints > 0) {
            log.finest("Number of mount points is: " + numMountPoints
              + ". Expanding path");
            final List<File> expandedFiles = expandEtlPathWithMountPoints(numMountPoints);
            log.finest("Expanded File list is: " + expandedFiles);
            for (File file : expandedFiles) {
              log.finest("Sending file : "
                + file.getAbsolutePath() + " for zipping");
              sendFilesForZipping(file, zff);

            }

          } else {
            log.finest("Single Directory Structure detected for zipping. No need to expand mount points.");
            sendFilesForZipping(inDir, zff);

          }

        } else {
          // Normal behaviour
          zff = new ZippingFileFilter(conf, log);
          recurseDirectory(inDir, 0, zff);
          zff.close();

        }

        log.info("Successfully managed " + zff.getFiles() + "/" + zff.getTotalFiles() + " files (" + zff.getBytes()
          + "B) in " + (System.currentTimeMillis() - execstart) + " ms");
      } else if (archiveMode == DELETED) {
        DeletingFileFilter dff = new DeletingFileFilter(conf, log);

        /**** Check if the delete is for failed dirs in $etldata/ ***/
        if (inDir.getAbsolutePath().contains("etldata")) {

          final int numMountPoints = CommonUtils.getNumOfDirectories(log);
          if (numMountPoints > 0) {
            log.finest("Number of mount points is" + numMountPoints + ". Expanding path");
            final List<File> expandedFiles = expandEtlPathWithMountPoints(numMountPoints);
            log.finest("Expanded File list is: " + expandedFiles);
            for (File file : expandedFiles) {
              log.finest("Sending file : " + file.getAbsolutePath() + " for deletion");
              sendFilesForDeletion(file, dff);
            }

          } else {
            log.finest("Single Directory Structure detected. No need to expand mount points.");
            sendFilesForDeletion(inDir, dff);
          }

          log.info("Successfully managed " + dff.getFiles() + "/" + dff.getTotalFiles() + " files in "
      	          + (System.currentTimeMillis() - execstart) + " ms");

        } else{
          // normal behaviour
       		Path dir = FileSystems.getDefault().getPath(inDir.getAbsolutePath());
       		recurseDeleteFiles(dir, dff);
        	
        	log.info("Successfully managed " + dff.getFiles() + "/" + dff.getTotalFiles() + " files in "
      	          + (System.currentTimeMillis() - execstart) + " ms");
		  }
      } else {
        throw new Exception("Unknown archive mode " + archiveMode);
      }

    } catch (Exception e) {
      log.log(Level.WARNING, "Manage " + inDir + " failed in " + (System.currentTimeMillis() - execstart) + " ms", e);
      throw e;
    }

  }


  /**
   * *
   * check Files For Deletion
   *
   * @param inDir usually $etldata
   * @param dff   Filter to get files to delete
   */

  private void sendFilesForDeletion(File inDir, DeletingFileFilter dff) {
    File[] files = inDir.listFiles();

    for (File dcDir : files) {
      if (dcDir.isDirectory()) {
        log.finest("Sending file: " + dcDir.getAbsolutePath() + " for checking age.");
        listAllFilesForDeletion(dcDir, dff);
      }
    }

  }


  /**
   * Delete files inside $etldata/failed dirs which are older than specified time..
   *
   * @param dcDir directory to list
   * @param dff   file filter
   */
  private void listAllFilesForDeletion(File dcDir, DeletingFileFilter dff) {

    File[] subdirs = dcDir.listFiles();
    for (File inDir : subdirs) {
      log.finest("Directory inDir is:" + inDir);

      //Look for failed directory in each of the DC_E_ABC directories..
      if (inDir.getAbsolutePath().contains("failed")) {
        log.finest("Inside failed directories in etldata");
        //Results stored in the file filter
        //noinspection ResultOfMethodCallIgnored
        inDir.listFiles(dff);
      } else {
        log.finest("No failed directory in:" + inDir.getAbsolutePath());
      }

    }

  }

  /**
   * @param inDir: usually $etldata
   * @param zff    file file filter
   */

  private void sendFilesForZipping(File inDir, ZippingFileFilter zff) {
    File[] files = inDir.listFiles();

    for (File dcDir : files) {
      if (dcDir.isDirectory()) {
        log.finest("Sending file: " + dcDir.getAbsolutePath() + " for age check");
        listAllFilesForZipping(dcDir, zff);
      }
    }

  }

  /**
   * Zip files inside $etldata/failed dirs which are older than specified time..
   *
   * @param dcDir directory to list
   * @param zff   zip file filter
   */
  private void listAllFilesForZipping(File dcDir, ZippingFileFilter zff) {

    File[] subdirs = dcDir.listFiles();
    for (File inDir : subdirs) {
      log.finest("Zipping Directory inDir is:" + inDir);

      //Look for failed directory in each of the DC_E_ABC directories..
      if (inDir.getAbsolutePath().contains("failed")) {
        log.finest("Inside failed directories in etldata for zipping");
        zff.targetDir = inDir;
        //Will check ZippingFileFilter::accept()
        //Results stored in the file filter
        //noinspection ResultOfMethodCallIgnored
        inDir.listFiles(zff);
        //zff.close();
      } else {
        log.finest("No failed directory found in:" + inDir.getAbsolutePath());
      }

    }

  }

  /**
   * **
   * <p/>
   * <p/>
   * Will create a list of directories based on the noOfDirs(i.e no of mount points specified in niq.ini) as below:
   * </eniq/data/etldata_/00/>
   * </eniq/data/etldata_/01/>
   * </eniq/data/etldata_/02/>
   * </eniq/data/etldata_/03/>
   *
   * @param numOfDirs number of mount points in etldata_
   * @return Mount points in etldata_
   */

  private static List<File> expandEtlPathWithMountPoints(final int numOfDirs) {
    final String etldata_ = System.getProperty(EVENTS_ETLDATA_DIR, EVENTS_ETLDATA_DIR_VAL);
    final File etlDataDir = new File(etldata_);
    final List<File> expandedFiles = new ArrayList<File>();
    for (int i = 0; i < numOfDirs; i++) {
      String mountNumber = i <= 9 ? "0" : "";
      mountNumber += i;
      final File expandedDir = new File(etlDataDir, mountNumber);
      if (!expandedFiles.contains(expandedDir)) {
        expandedFiles.add(expandedDir);
      }
    }
    return expandedFiles;
  }


  /**
   * Recurse one directory.
   */
  private void recurseDirectory(final File filedir, final int depth, final FileFilter filter) {
    if (filedir == null) {
      log.fine("File is null!");
    } else if (!filedir.exists()) {
      log.fine("File " + filedir.getPath() + " does not exist!");
    } else if (depth <= maxDepth) {
      log.fine("Managing subdirectory " + filedir.getPath());
      final File[] subdirs = filedir.listFiles(filter);
      if (subdirs == null) {
        log.warning("Listing for " + filedir.getPath() + " returned null, ignoring!");
      } else {
        if (deleteEmptyDirs) {
          log.fine("Checking directory " + filedir.getPath() + " for deletion");
          final String[] containedFiles = filedir.list();
          if (containedFiles == null) {
            log.log(Level.WARNING, "Empty listing failed, null value returned for " + filedir.getPath());
          } else if (containedFiles.length == 0) {
            if (filedir.delete()) {
              log.info("Directory " + filedir.getPath() + " deleted");
            } else {
              log.warning("Cannot delete directory " + filedir.getPath() + ", still contains " +
                containedFiles.length + " files?");
            }
          } else {
            log.finer("Directory " + filedir.getPath() + " is not empty?");
          }
        }
        log.fine("Subdirectory " + filedir.getPath() + " managed");
        for (File subdir : subdirs) {
          recurseDirectory(subdir, depth + 1, filter);
        }
      }
    }
  }
  
  public void recurseDeleteFiles(Path filedir, DeletingFileFilter filter) throws IOException{
	DirectoryStream<Path> stream = null;
	File file = null;
  	try {
  		log.finest("Managing subdirectory " + filedir);
		stream = Files.newDirectoryStream( filedir );
	} catch (IOException e) {
		e.printStackTrace();
	}
  	
  	if (stream != null){
		for (Path pathObj : stream) {
			if (Files.isDirectory(pathObj)) {
				log.finest("Subdirectory " + filedir + " managed ");
				recurseDeleteFiles(pathObj, filter);
			}
			else {
				file = pathObj.toFile();
				if (file != null) {
				try {
					if(filter.ageCheck(file) > 0 ){
						log.finest("Deleting file :" + file );
						if(!file.getName().equals(".tagfile")) { // HP49595, diskmanager should skip .tagfile(used by NAS)deletion
							boolean suc = file.delete(); 
							if(!suc){
								log.finest("Cannot delete file " + file);									}
							}	
						}	
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		stream.close();
		}
  	}
}
