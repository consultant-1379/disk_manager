package com.distocraft.dc5000.diskmanager;

import java.io.File;
import java.io.FileFilter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.distocraft.dc5000.common.StaticProperties;


/**
 * Abstract super class for all file filters.
 * 
 * Copyright Ab Ericsson Oy 2007
 * 
 * @author etuolem
 */

public abstract class DiskManagerFileFilter implements FileFilter {

  public static final int FILE_SYSTEM_AGE = 0;

  public static final int FILENAME_AGE = 1;

  protected long fileAge = 0L;

  private int mode = 0;

  private Pattern timePattern = null;

  private SimpleDateFormat dateFormat = null;

  private Calendar calendar = null;

  private int currentyear = 0;
  
  private Pattern fileMask = null;

  // This is fixed so that if zip file name, if start and stop times
  // are used in name, would be as consistent as possible

  protected long start = System.currentTimeMillis();

  // Performance counters
  
  protected int totalfiles = 0;
  
  protected int files = 0;
  
  long bytes = 0;
  
  DiskManagerFileFilter(Properties conf) throws Exception {

    final long fileAgeHour = parsePropertyAsLong(conf.getProperty("diskManager.dir.fileAgeHour", "0"), "diskManager.dir.fileAgeHour");
    final long fileAgeDay = parsePropertyAsLong(conf.getProperty("diskManager.dir.fileAgeDay", "0"),"diskManager.dir.fileAgeDay");
    final long fileAgeMinutes = parsePropertyAsLong(conf.getProperty("diskManager.dir.fileAgeMinutes", "0"), "diskManager.dir.fileAgeMinutes");

    fileAge = (((fileAgeDay * 24L) + fileAgeHour) * 3600000L) + (fileAgeMinutes * 60000L);

    mode = Integer.parseInt(conf.getProperty("diskManager.dir.fileAgeMode", String.valueOf(FILE_SYSTEM_AGE)));

    if (mode == FILENAME_AGE) {
      String spat = conf.getProperty("diskManager.dir.timeMask");

      if (spat == null || spat.length() <= 0) {
        throw new Exception("Parameter timeMask must be defined");
      }

      try {
        timePattern = Pattern.compile(conf.getProperty("diskManager.dir.timeMask"));
      } catch (Exception e) {
        throw new Exception("TimeMask parameter \"" + timePattern + "\" is invalid");
      }

      final String sdf = conf.getProperty("diskManager.dir.dateFormatInput");

      if (sdf == null || sdf.length() <= 0) {
          throw new Exception("Parameter dateFormatInput must be defined");
      }

      try {
        dateFormat = new SimpleDateFormat(sdf);
      } catch (Exception e) {
        throw new Exception("Dateformat parameter \"" + dateFormat + "\" is invalid");
      }

      calendar = Calendar.getInstance();
      calendar.setTimeInMillis(start);

      currentyear = calendar.get(Calendar.YEAR);

    } else if (mode == FILE_SYSTEM_AGE) {
      // OK do nothing.
    } else {
      throw new Exception("Unknown FileAgeMode " + mode);
    }
    
    String sfileMask = conf.getProperty("diskManager.dir.fileMask");
    if (sfileMask == null || sfileMask.length() <= 0) {
      throw new Exception("Parameter fileMask not defined");
    }
  
    try {
      fileMask = Pattern.compile(sfileMask);
    } catch (Exception e) {
      throw new Exception("FileMask parameter \"" + timePattern + "\" is invalid");
    }
  
  }

  /**
   * In Here just to implement FileFilter interface.
   */

  @Override
  public abstract boolean accept(File f);
  
  /**
   * Returns total amount of files checked.
   */
  int getTotalFiles() {
    return totalfiles;
  }
  
  /**
   * Returns total amount of files handler.
   */
  int getFiles() {
    return files;
  }
  
  /**
   * Returns total number of bytes written while moving or zipping.
   */
  long getBytes() {
    return bytes;
  }

  /**
   * Checks if file date. Returns date of file or -1 if not to be archived.
   */
  long ageCheck(File file) throws Exception {
    
    totalfiles++;
    
    long filemodtime = -1;
    
    Matcher m = fileMask.matcher(file.getName());



    if (!m.matches()){
      return filemodtime;
    }

    if (mode == FILE_SYSTEM_AGE) { // check from FS

      filemodtime = file.lastModified();

    } else if (mode == FILENAME_AGE) { // parse from fileName

      Matcher matcher = timePattern.matcher(file.getName());
      matcher.find();
      String time = matcher.group(1);

      calendar.setTime(dateFormat.parse(time));

      // if files year is 1970 (eg. no date is found from datetime) insert
      // current year
      if (calendar.get(Calendar.YEAR) == 1970) {
        calendar.set(Calendar.YEAR, currentyear);

        // if given date is from future, reduce year by 1 ...
        if (calendar.getTimeInMillis() > start){
          calendar.add(Calendar.YEAR, -1);
		}

      }

      filemodtime = calendar.getTimeInMillis();

    }


    if (filemodtime==0){
      // 0L if the file does not exist (if a symbolic link exists but real files does not).
      // We want to remove the file in this case. 
      files++;
      return 1L;      
    } else if ((start - filemodtime) > fileAge) {
      files++;
      return filemodtime;
    } else {
      return -1;
    }
      
  }

  /**
   * Get config setting from action_contents as a long.
   * 
   * If setting is defined as a number, return as long.
   * 
   * If setting is defined as static.property, parse the
   * static.properties key and the default value. Look up
   * the key in static.properties and use the value if found, 
   * otherwise use the default.
   * 
   * @param property Value of action_contents property 
   * @param name Reference name of action_contents property
   * @return parsed property value
 * @throws Exception 
   */
  final long parsePropertyAsLong(final String property, final String name) throws Exception {
    long propertyValue = -1;
    
    if (property != null && !property.trim().equals("")) {

      boolean propIsLong = true;
      
      try {
        propertyValue = Long.valueOf(property);
      } catch (Exception e) {
        propIsLong = false;
      }
      
      /** if property is not a long, check for 
       * static.properties key */
      if (!propIsLong) {
        Pattern staticPropertyFormat = Pattern.compile("([a-zA-Z][\\w.]*[\\w]):([0-9]+)");
          Matcher spMatcher = staticPropertyFormat.matcher(property);
          if (spMatcher.matches()) {
            String staticPropKey = spMatcher.group(1);
            String defaultVal = spMatcher.group(2);
            
            // lookup value in static.properties
            String staticPropValue = StaticProperties.getProperty(staticPropKey, defaultVal);
            try {
              propertyValue = Long.valueOf(staticPropValue);
            } catch (Exception e) {
              throw new Exception("Value of static.property [" + staticPropKey + "] for action_contents property [" + name +
                                "] is not a valid long [" + staticPropValue + "]");
            }
          } else {
          throw new Exception("Value for action_contents property [" + name +
                      "] is not a valid long or valid static property key [" + property + "]");
          }
      }
    }   
    
    return propertyValue;
  }
}
