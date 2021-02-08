/*
 * (c) BRY-IT GmbH
 * http://www.bry-it.com
 */
package bry.mogo.testing;

import java.sql.Connection;

import org.dbunit.IDatabaseTester;
import org.dbunit.database.IDatabaseConnection;
import org.junit.Test;

/**
 *
 * @author mschlichtmann
 */
public class DbUnitMogoDriverTest {

  /**
   * Test of getConnection method, of class DbUnitMogoDriver.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testGetConnection() throws Exception {
    Connection aConnection = DbUnitMogoDriver.getConnection("jdbc:oracle:thin:@v04win2003:1521:live2");
    org.junit.Assert.assertNotNull(aConnection);
  }

  /**
   * Test of getDatabaseConnection method, of class DbUnitMogoDriver.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testGetDatabaseConnection() throws Exception {
    IDatabaseConnection aConnection = DbUnitMogoDriver.getDatabaseConnection("MGF", DbUnitMogoDriver.DBType.Oracle);
    org.junit.Assert.assertNotNull(aConnection);
  }

  /**
   * Test of getDbTester method, of class DbUnitMogoDriver.
   *
   * @throws java.lang.Exception
   */
  @Test
  public void testGetDbTester() throws Exception {
    DbUnitMogoDriver driver = new DbUnitMogoDriver();
    org.junit.Assert.assertNotNull(driver);
    IDatabaseConnection dbConnection = DbUnitMogoDriver.getDatabaseConnection("MGF", DbUnitMogoDriver.DBType.Oracle);
    org.junit.Assert.assertNotNull(dbConnection);
    DbUnitMogoDriver.TestDbConfig aTestDbConfig = driver.new TestDbConfig("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@v04win2003:1521:live2", "MGF", "MGF", "Heiler01");
    driver.setMogoConfig(aTestDbConfig);
    IDatabaseTester aIDatabaseTester = driver.getDbTester(driver.getMogoConfig(), DbUnitMogoDriver.TestType.jdbc);
    org.junit.Assert.assertNotNull(aIDatabaseTester);
  }

  /**
   * Test of getMogoConfig method, of class DbUnitMogoDriver.
   */
  @Test
  public void testGetSetMogoConfig() {
    DbUnitMogoDriver driver = new DbUnitMogoDriver();
    org.junit.Assert.assertNotNull(driver);      
    DbUnitMogoDriver.TestDbConfig anotherTestDbConfig = driver.getMogoConfig();
    org.junit.Assert.assertNotNull(anotherTestDbConfig);
  }

}
