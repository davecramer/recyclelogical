package org.postgresql;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.ServerVersion;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Hello world!
 *
 */
public class App
{
  private static final String SLOT_NAME = "pgjdbc_logical_replication_slot";
  private static final String SERVER_HOST_PORT_PROP = "hostport";
  private static final String DATABASE_PROP = "database";
  private Connection replConnection;
  private Connection sqlConnection;

  public static void main( String[] args ) throws  Exception
  {
    Properties properties = new Properties();
    properties.setProperty("username", "test");
    properties.setProperty("password", "test");
    properties.setProperty("database", "test");
    properties.setProperty("host", "localhost");
    App app = new App();
    try {
      app.setUp(properties);
      app.testReceiveChangesAfterStartReplication();
    }finally{
      app.tearDown();
    }
  }

  public void setUp(Properties properties) throws Exception {
    sqlConnection = openDB(properties);
    replConnection = openReplicationConnection(properties);

    executeSQL(sqlConnection, "create table test_logic_table (pk serial primary key, name varchar(100))");

    recreateLogicalReplicationSlot(sqlConnection, SLOT_NAME, "test_decoding");
  }

  public void tearDown() throws Exception {
    replConnection.close();
    executeSQL(sqlConnection, "drop table test_logic_table");
    dropReplicationSlot(sqlConnection, SLOT_NAME);
    sqlConnection.close();
  }
  private void executeSQL(Connection con, String sql){
    try (Statement stmt = con.createStatement()){
      stmt.executeQuery(sql);
    }catch( SQLException ex ){

    }
  }


  public void testReceiveChangesAfterStartReplication() throws Exception {
    PGConnection pgConnection = (PGConnection) replConnection;

    LogSequenceNumber lsn = getCurrentLSN();

    PGReplicationStream stream =
        pgConnection
            .getReplicationAPI()
            .replicationStream()
            .logical()
            .withSlotName(SLOT_NAME)
            .withStartPosition(lsn)
            .withSlotOption("include-xids", false)
            .withSlotOption("skip-empty-xacts", true)
            .start();


    List<String> result = new ArrayList<String>();

    Statement st = sqlConnection.createStatement();
    st.execute(
        "insert into test_logic_table(name) values('first message after start replication')");
    st.close();

    result.addAll(receiveMessage(stream, 3));

    st = sqlConnection.createStatement();
    st.execute(
        "insert into test_logic_table(name) values('second message after start replication')");

    result.addAll(receiveMessage(stream, 3));

    stream.close();

    lsn = getCurrentLSN();

    st.execute(
        "insert into test_logic_table(name) values('first message after start replication')");
    st.close();

    stream =
        pgConnection
            .getReplicationAPI()
            .replicationStream()
            .logical()
            .withSlotName(SLOT_NAME)
            .withStartPosition(lsn)
            .withSlotOption("include-xids", false)
            .withSlotOption("skip-empty-xacts", true)
            .start();

    System.out.println( "Slot should be active " + isReplicationSlotActive(sqlConnection, SLOT_NAME));
    stream.close();


  }
  private List<String> receiveMessage(PGReplicationStream stream, int count) throws SQLException {
    List<String> result = new ArrayList<String>(count);
    for (int index = 0; index < count; index++) {
      result.add(toString(stream.read()));
    }

    return result;
  }
  private String group(List<String> messages) {
    StringBuilder builder = new StringBuilder();
    boolean isFirst = true;
    for (String str : messages) {
      if (isFirst) {
        isFirst = false;
      } else {
        builder.append("\n");
      }

      builder.append(str);
    }

    return builder.toString();
  }

  private LogSequenceNumber getCurrentLSN() throws SQLException {
    Statement st = sqlConnection.createStatement();
    ResultSet rs = null;
    try {
      rs = st.executeQuery("select "
          + (((BaseConnection) sqlConnection).haveMinimumServerVersion(ServerVersion.v10)
          ? "pg_current_wal_lsn()" : "pg_current_xlog_location()"));

      if (rs.next()) {
        String lsn = rs.getString(1);
        return LogSequenceNumber.valueOf(lsn);
      } else {
        return LogSequenceNumber.INVALID_LSN;
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
      st.close();
    }
  }
  private Connection openDB(Properties props) throws SQLException{
    // Allow properties to override the user name.
    String user = props.getProperty("username");
    props.setProperty("user", user);
    String password = props.getProperty("password");
    props.setProperty("password", password);

    if (!props.containsKey(PGProperty.PREFER_QUERY_MODE.getName())) {
      String value = System.getProperty(PGProperty.PREFER_QUERY_MODE.getName());
      if (value != null) {
        props.put(PGProperty.PREFER_QUERY_MODE.getName(), value);
      }
    }
    String hostport = props.getProperty(SERVER_HOST_PORT_PROP, getServer() + ":" + getPort());
    String database = props.getProperty(DATABASE_PROP, getDatabase());

    return DriverManager.getConnection(getURL(hostport, database), props);

  }
  public static String getServer() {
    return System.getProperty("server", "localhost");
  }

  /*
   * Returns the Test port
   */
  public static int getPort() {
    return Integer.parseInt(System.getProperty("port", "5432"));
  }

  private static String getURL(String hostport, String database){
    return "jdbc:postgresql://"+hostport+'/'+ database;
  }

  private Connection openReplicationConnection(Properties properties) throws Exception {
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
//    PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
    PGProperty.REPLICATION.set(properties, "database");
    return openDB(properties);
  }
  private static String toString(ByteBuffer buffer) {
    int offset = buffer.arrayOffset();
    byte[] source = buffer.array();
    int length = source.length - offset;

    return new String(source, offset, length);
  }
  /*
   * Returns the Test database
   */
  public static String getDatabase() {
    return System.getProperty("database");
  }

  /*
   * Returns the Postgresql username
   */
  public static String getUser() {
    return System.getProperty("username");
  }

  /*
   * Returns the user's password
   */
  public static String getPassword() {
    return System.getProperty("password");
  }

  public static boolean isReplicationSlotActive(Connection connection, String slotName)
      throws SQLException {

    try (PreparedStatement stm=connection.prepareStatement("select active from pg_replication_slots where slot_name = ?") ){
      stm.setString(1, slotName);
      try( ResultSet rs = stm.executeQuery()){
        return rs.next() && rs.getBoolean(1);
      }
    }
  }

  private static void waitStopReplicationSlot(Connection connection, String slotName)
      throws InterruptedException, TimeoutException, SQLException {
    long startWaitTime = System.currentTimeMillis();
    boolean stillActive;
    long timeInWait = 0;

    do {
      stillActive = isReplicationSlotActive(connection, slotName);
      if (stillActive) {
        TimeUnit.MILLISECONDS.sleep(100L);
        timeInWait = System.currentTimeMillis() - startWaitTime;
      }
    } while (stillActive && timeInWait <= 30000);

    if (stillActive) {
      throw new TimeoutException("Wait stop replication slot " + timeInWait + " timeout occurs");
    }
  }

  public static void dropReplicationSlot(Connection connection, String slotName)
      throws SQLException, InterruptedException, TimeoutException {
      try (PreparedStatement stm=connection.prepareStatement("select pg_terminate_backend(active_pid) from pg_replication_slots "
                + "where active = true and slot_name = ?")) {
        stm.setString(1, slotName);
        stm.execute();
      }

    waitStopReplicationSlot(connection, slotName);

    try (PreparedStatement stm = connection.prepareStatement(
          "select pg_drop_replication_slot(slot_name) "
              + "from pg_replication_slots where slot_name = ?")){
      stm.setString(1, slotName);
      stm.execute();
    }
  }

  public static void recreateLogicalReplicationSlot(Connection connection, String slotName, String outputPlugin)
      throws SQLException, TimeoutException, InterruptedException {
    //drop previous slot
    dropReplicationSlot(connection, slotName);

    try (PreparedStatement stm = connection.prepareStatement("SELECT * FROM pg_create_logical_replication_slot(?, ?)")){
      stm.setString(1, slotName);
      stm.setString(2, outputPlugin);
      stm.execute();
    }
  }
}
