package com.distocraft.dc5000.diskmanager;

import static org.junit.Assert.*;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Logger;

import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author edeamai
 * 
 */
public class DeletingFileFilterTest {

  DeletingFileFilter objectUnderTest;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {

    new File("Indir").mkdir();

    File f1, f2, f3;
    f1 = new File("Indir/test.log");
    f1.createNewFile();
    f2 = new File("Indir/.tagfile");
    f2.createNewFile();

    // print the original last modified date
    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

    // set this date
    String newLastModified = "01/31/1998";

    // need convert the above date to milliseconds in long value
    Date newDate = sdf.parse(newLastModified);

    f1.setLastModified(newDate.getTime());
    f2.setLastModified(newDate.getTime());

  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    File f3 = new File("Indir/.tagfile");
    f3.delete();

    final String SRC_FOLDER = "Indir";
    File directory = new File(SRC_FOLDER);
    directory.delete();

  }

  @Test
  public void testAccept() {

    Logger mockedLog;
    final File mockedFile;
    final Mockery context = new JUnit4Mockery();

    {
      // we need to mock classes, not just interfaces.
      context.setImposteriser(ClassImposteriser.INSTANCE);
    }

    Properties prop = new Properties();
    prop.setProperty("diskManager.dir.fileMask", ".*");
    prop.setProperty("diskManager.dir.fileAgeDay", "65");
    mockedLog = context.mock(Logger.class);
    try {
      objectUnderTest = new DeletingFileFilter(prop, mockedLog);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    File filedir = new File("Indir");
    File[] subdirs = filedir.listFiles(objectUnderTest);

    File dir = new File("Indir");
    File[] files = dir.listFiles();
    for (int i = 0; i < files.length; i++) {

      if (files[i].isFile() && files[i].getName().equals(".tagfile")) {

        assertTrue("The tagfile should not be deleted ", files[i].getName().equals(".tagfile"));
      }

    }
  }
}
