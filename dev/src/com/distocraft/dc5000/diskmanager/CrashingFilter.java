package com.distocraft.dc5000.diskmanager;

import java.io.File;
import java.io.FileFilter;

class CrashingFilter implements FileFilter {

  public boolean accept(File arg0) {
    throw new NullPointerException("FILES!");
  }

}
