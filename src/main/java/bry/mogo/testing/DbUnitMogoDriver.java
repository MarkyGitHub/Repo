/*
 * (c) BRY-IT GmbH
 * http://www.bry-it.com
 */
package bry.mogo.testing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.ext.oracle.OracleDataTypeFactory;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import static bry.mogo.testing.DbUnitMogoDriver.getDbTester;

/**
 * Um Datenbank getriebene entwicklung zu ermöglichen. Die Klasse ermöglicht den Datenbank vor und nach jedem Testlauf
 * die Datenbank in einem definierten Zustand zu haben.
 *
 * @author mschlichtmann
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class DbUnitMogoDriver {

  /**
   * Encapsulates the database connection details.
   */
  @Getter
  @Setter
  @AllArgsConstructor
  public class TestDbConfig {

    private String driverClass;

    private String connectionUrl;

    private String schemaName;

    private String username;

    private String password;
  }

  @Getter
  @Setter
  private TestDbConfig mogoConfig = new TestDbConfig("oracle.jdbc.driver.OracleDriver",
      "jdbc:oracle:thin:@v04win2003:1521:live2", "MGF", "MGF", "Heiler01");

  /**
   * Enumareations for a few selected Databases availble.
   */
  public enum DBType {
    Oracle,
    MySql,
    JavaDb,
    Derby;
  }

  /**
   * The database statement test type.
   */
  public enum TestType {
    jdbc,
    jndi,
    dataSource,
    prep;
  }

  /**
   * To get the @Connection for a given database connection url.
   *
   * @param pUrl
   * @return Connection
   * @throws SQLException
   */
  public static Connection getConnection(String pUrl) throws SQLException {
//    Class driverClass = Class.forName(_driverClass);
    Connection connection = DriverManager.getConnection(pUrl, "MGF", "Heiler01");
    return connection;
  }

  /**
   * Connections können mit ganzen Datenbanken und tabellen umgehen, um so das Bedienen von großen Datenmengen zu
   * erleichtern.
   *
   * @param pSchemaName
   * @param pDb
   * @return
   * @throws SQLException
   * @throws DatabaseUnitException
   */
  public static IDatabaseConnection getDatabaseConnection(String pSchemaName, DBType pDb) throws SQLException, DatabaseUnitException {
    IDatabaseConnection connection = null;
    DatabaseConfig config = null;
    Connection aConnection = getConnection("jdbc:oracle:thin:@v04win2003:1521:live2");
    switch (pDb) {
      case Oracle:
        connection = new DatabaseConnection(aConnection, pSchemaName);
        config = connection.getConfig();
        config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());
        break;
      case MySql:
        connection = new DatabaseConnection(aConnection, pSchemaName);
        config = connection.getConfig();
        config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());
        break;
      case JavaDb:
        connection = new DatabaseConnection(aConnection, pSchemaName);
        config = connection.getConfig();
        config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());
        break;
      case Derby:
        connection = new DatabaseConnection(aConnection, pSchemaName);
        config = connection.getConfig();
        config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());
        break;
      default:
        break;
    }
    return connection;
  }

  /**
   * Initializes a @IDatabaseTester object for usage in DB driven Test scenarios.
   *
   * @param pConfig
   * @param pTestType
   * @return
   * @throws ClassNotFoundException
   */
  public static IDatabaseTester getDbTester(final TestDbConfig pConfig, final TestType pTestType) throws
      Exception {
    IDatabaseTester databaseTester = null;
    switch (pTestType) {
      case jdbc:
        databaseTester = new JdbcDatabaseTester(pConfig.getDriverClass(), pConfig.getConnectionUrl(),
            pConfig.getUsername(), pConfig.getPassword(), pConfig.getSchemaName());
        break;
      case jndi:
        databaseTester = new JdbcDatabaseTester(pConfig.getDriverClass(), pConfig.getConnectionUrl(),
            pConfig.getUsername(), pConfig.getPassword(), pConfig.getSchemaName());
        break;
      case dataSource:
        databaseTester = new JdbcDatabaseTester(pConfig.getDriverClass(), pConfig.getConnectionUrl(),
            pConfig.getUsername(), pConfig.getPassword(), pConfig.getSchemaName());
        break;
      case prep:
        databaseTester = new JdbcDatabaseTester(pConfig.getDriverClass(), pConfig.getConnectionUrl(),
            pConfig.getUsername(), pConfig.getPassword(), pConfig.getSchemaName());
        break;
      default:
        break;
    }

    databaseTester.setDataSet(databaseTester.getConnection().createDataSet());
    return databaseTester;
  }

  /**
   *
   * @param pConfig
   * @param pTestType
   *    * @return IDataSet
   * @return
   * @throws Exception
   */
  public static IDataSet getDataset(final TestDbConfig pConfig,
      final TestType pTestType) throws Exception {
    final IDatabaseTester dbTester = getDbTester(pConfig, pTestType);
    dbTester.setDataSet(dbTester.getConnection().createDataSet());
    return dbTester.getDataSet();

  }

  /**
   *
   * @param pConfig
   * @param pTestType
   *    * @return IDataSet
   * @param table
   * @return
   * @throws Exception
   */
  public static ITable getTable(final DbUnitMogoDriver.TestDbConfig pConfig,
      final DbUnitMogoDriver.TestType pTestType, String table) throws Exception {
    final IDatabaseTester dbTester = getDbTester(pConfig, pTestType);
    dbTester.setDataSet(dbTester.getConnection().createDataSet());
    return getDbTester(pConfig, pTestType).getDataSet().getTable(table);

  }

}
