package com.distocraft.dc5000.diskmanager;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.distocraft.dc5000.common.StaticProperties;

public class DiskManagerFileFilterTest {

    Properties undefinedProps = new Properties();
    Properties simpleProps = new Properties();
    Properties advancedProps = new Properties();

	@Before
	public void setUp() throws Exception {
		undefinedProps.setProperty("diskManager.dir.fileMask", ".*");

		simpleProps.setProperty("diskManager.dir.fileAgeHour", "6");
		simpleProps.setProperty("diskManager.dir.fileAgeDay", "30");
		simpleProps.setProperty("diskManager.dir.fileAgeMinutes", "15");
		simpleProps.setProperty("diskManager.dir.fileMask", ".*");

		advancedProps.setProperty("diskManager.dir.fileAgeHour", "serviceAudit.fileAgeHour:10");
		advancedProps.setProperty("diskManager.dir.fileAgeDay", "serviceAudit.fileAgeDay:65");
		advancedProps.setProperty("diskManager.dir.fileAgeMinutes", "serviceAudit.fileAgeMinutes:45");
		advancedProps.setProperty("diskManager.dir.fileMask", ".*");

		final Properties staticProps = new Properties();
		staticProps.setProperty("serviceAudit.fileAgeHour", "1");
		staticProps.setProperty("serviceAudit.fileAgeDay", "1");
		staticProps.setProperty("serviceAudit.fileAgeMinutes", "1");
		StaticProperties.giveProperties(staticProps);
	}

	/**
	 * When "fileAge" properties are not defined in action_contents, DiskManagerFileFilter defaults the value to 0
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSetsAgeCorrectlyForUndefinedProps() throws Exception {
		MockedDiskManagerFileFilter dmff = new MockedDiskManagerFileFilter(undefinedProps);
		long expectedFileAge = (((0 * 24L) + 0) * 3600000L) + (0 * 60000L);
		
		assertEquals(expectedFileAge, dmff.fileAge);
	}

	/**
	 * Check that fileAge is set correctly when "fileAge" properties are defined as numbers
	 * @throws Exception
	 */
	@Test
	public void testSetsAgeCorrectlyForSimpleProps() throws Exception {
		MockedDiskManagerFileFilter dmff = new MockedDiskManagerFileFilter(simpleProps);
		long expectedFileAge = (((30 * 24L) + 6) * 3600000L) + (15 * 60000L);
		
		assertEquals(expectedFileAge, dmff.fileAge);
	}

	/**
	 * Check that fileAge is set correctly when "fileAge" properties are defined as static.properties
	 * but the static.properties do not exist. Associated default values should be used
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSetsAgeCorrectlyForAdvancedProps() throws Exception {
		MockedDiskManagerFileFilter dmff = new MockedDiskManagerFileFilter(advancedProps);
		long expectedFileAge = (((1 * 24L) + 1) * 3600000L) + (1 * 60000L);
		
		assertEquals(expectedFileAge, dmff.fileAge);
	}

	/**
	 * Check that fileAge is set correctly when "fileAge" properties are defined as static.properties
	 * and the static.properties exist
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSetsAgeCorrectlyForAdvancedPropsMissingSPs() throws Exception {

		// clear static.properties for test
		final Properties staticProps = new Properties();
		StaticProperties.giveProperties(staticProps);
		
		MockedDiskManagerFileFilter dmff = new MockedDiskManagerFileFilter(advancedProps);
		long expectedFileAge = (((65 * 24L) + 10) * 3600000L) + (45 * 60000L);
		
		assertEquals(expectedFileAge, dmff.fileAge);
	}
	
	/** 
	 * Check that an exception is thrown when a "fileAge" property is invalid in action_contents  
	 * @throws Exception
	 */
	@Test
	public void testInvalidProperty() throws Exception {
		simpleProps.setProperty("diskManager.dir.fileAgeDay", "badProperty");
		
		String expectedMsg = "Value for action_contents property [diskManager.dir.fileAgeDay] is not a valid long or valid static property key [badProperty]";
		String actualmsg = "";
		
		try {
			MockedDiskManagerFileFilter dmff = new MockedDiskManagerFileFilter(simpleProps);
		} catch (Exception e) {
			actualmsg = e.getMessage();
		} finally {
			assertEquals("Expected exception not thrown.", expectedMsg, actualmsg);
		}
	}

	/**
	 * Check that an exception is thrown when the value returned for a static.property referenced by a "fileAge" property is invalid
	 *  
	 * e.g. In action_contents: diskManager.dir.fileAgeDay=serviceAudit.fileAgeDay:20
	 * In static.properties: serviceAudit.fileAgeDay=badSP
	 * 
	 * The expected result is DiskManagerFileFilter looks up the static property "serviceAudit.fileAgeDay" gets "badSP" and 
	 * throws an exception.
	 *  
	 * @throws Exception
	 */
	@Test
	public void testInvalidStaticProperty() throws Exception {

		String expectedMsg = "Value of static.property [serviceAudit.fileAgeDay] for action_contents property [diskManager.dir.fileAgeDay] is not a valid long [badSP]";
		String actualmsg = "";
		
		final Properties staticProps = new Properties();
		staticProps.setProperty("serviceAudit.fileAgeHour", "1");
		staticProps.setProperty("serviceAudit.fileAgeDay", "badSP");
		staticProps.setProperty("serviceAudit.fileAgeMinutes", "1");
		StaticProperties.giveProperties(staticProps);

		try {
			MockedDiskManagerFileFilter dmff = new MockedDiskManagerFileFilter(advancedProps);
		} catch (Exception e) {
			actualmsg = e.getMessage();
		} finally {
			assertEquals("Expected exception not thrown.", expectedMsg, actualmsg);
		}
	}
}

/**
 * DiskManagerFileFilter is abstract so we need to extend it to test it.
 */
class MockedDiskManagerFileFilter extends DiskManagerFileFilter{

	MockedDiskManagerFileFilter(Properties conf) throws Exception {
		super(conf);
	}

	@Override
	public boolean accept(File f) {
		// Leave testing of accept to sub-classes that implement it
		return false;
	}
}