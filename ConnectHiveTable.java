package com.anand.readHive.readHive;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;



import com.anand.readHive.readHive.Constants;
import com.anand.readHive.readHive.CommonUtil;
import com.anand.readHive.readHive.ClusterData;


public class ConnectHiveTable {

	private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	private static final String TABLE_NAME = "Regression_Test";
	
	/*
	 * private static final String TABLE_COLUMNS =
	 * " (id BIGINT, timestamp TIMESTAMP, " +
	 * "clusterName STRING, clusterDistribution STRING, " +
	 * "regressionTestName STRING, regressionTestStart STRING, regressionTestEnd STRING, "
	 * +
	 * "componentName STRING, componentTestStart TIMESTAMP, componentTestEnd TIMESTAMP, "
	 * + "testCaseName STRING, testCaseStart TIMESTAMP, testCaseEnd TIMESTAMP, "
	 * +
	 * "cpuUtilization STRING, memoryUtilization STRING, networkIO STRING, diskIO STRING, error STRING)"
	 * ;
	 */
	private static final String TABLE_COLUMNS = " (id BIGINT, Timestamp TIMESTAMP, "
			+ "RegressionTestStart STRING, RegressionTestEnd STRING, "
			+ "ComponentTestStart TIMESTAMP, componentTestEnd TIMESTAMP, "
			+ "TestCaseStart TIMESTAMP, TestCaseEnd TIMESTAMP, "
			+ "MemoryUtilization BIGINT, VCoreUtilization INT, CpuUtilization DECIMAL(5,2), NetworkIO BIGINT, DiskIO BIGINT, Error STRING)";

	private String tableLocation;
	private Connection con;
	private Statement stmt;

	public ConnectHiveTable(String hostName, String realm) {

		try {
			Class.forName(DRIVER_NAME);

			con = DriverManager.getConnection("jdbc:hive2://" + hostName
					+ ":10000/default;principal=hive/_HOST@" + realm);

			stmt = con.createStatement();

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	@SuppressWarnings("unused")
	private static Connection getConnection(Subject signedOnUserSubject,
			final String hostName, final String realm) throws Exception {
		Connection conn = (Connection) Subject.doAs(signedOnUserSubject,
				new PrivilegedExceptionAction<Object>() {
					public Object run() {
						Connection con = null;
						String JDBC_DB_URL = "jdbc:hive2://"
								+ hostName
								+ ":10000/default;principal=hive/_HOST@"
								+ realm
								+ ";auth=kerberos;kerberosAuthType=fromSubject;";
						try {
							Class.forName(DRIVER_NAME);
							con = DriverManager.getConnection(JDBC_DB_URL);
						} catch (SQLException e) {
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
						return con;
					}
				});
		return conn;
	}

	public ConnectHiveTable(Connection con) {

		try {
			this.con = con;
			stmt = con.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	public ConnectHiveTable(String hostName, String realm, String username,
			String password) {

		System.out.println("ConnectHiveTable() ...");

		try {
			Class.forName(DRIVER_NAME);

			/*
			 * con = DriverManager .getConnection( "jdbc:hive2://" + hostName +
			 * ":10000/;ssl=true;transportMode=http;httpPath=gateway/default/hive"
			 * , username, password);
			 */
			con = DriverManager.getConnection("jdbc:hive2://" + hostName
					+ ":10000/;principal=hive/_HOST@" + realm, username,
					password);
		
			
			System.out.println("Connection=" + con);
			stmt = con.createStatement();

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}

	protected ConnectHiveTable(String hostName, String realm,
			String tableLocation) {
		this(hostName, realm);
		this.tableLocation = tableLocation;
	}

	private void createTable() throws SQLException {

		stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
		stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS "
				+ TABLE_NAME
				+ TABLE_COLUMNS
				+ " PARTITIONED BY (ClusterName STRING, ClusterDistribution STRING, RegressionTestName STRING, ComponentName STRING, TestCaseName STRING)"
				+ " row format delimited fields terminated by \",\" LOCATION '"
				+ tableLocation + "' ");

		System.out.println(TABLE_NAME + " was created!");
	}

	public void loadData(String filePath, String clusterName,
			String clusterDistribution, String regressionTestName,
			String componentName, String testCaseName) {

		// Partitions
		String partitions = " PARTITION ("
				+ getPartitionsComma(clusterName, clusterDistribution,
						regressionTestName, componentName, testCaseName) + ")";

		try {

			// load data into table
			String sql = "LOAD DATA LOCAL INPATH '" + filePath
					+ "' OVERWRITE INTO TABLE " + TABLE_NAME + partitions;

			System.out.println("sql=" + sql);
			stmt.execute(sql);

			// Add Partition
			sql = "ALTER TABLE " + TABLE_NAME + " ADD IF NOT EXISTS "
					+ partitions;
			stmt.execute(sql);

		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void showTables() throws SQLException {

		// show tables
		String sql = "show tables '" + TABLE_NAME + "'";
		System.out.println("***** " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// describe table
		sql = "describe " + TABLE_NAME;
		System.out.println("***** " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// show create table
		sql = "show create table " + TABLE_NAME;
		System.out.println("***** " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}

	}

	public List<ClusterData> queryData(String column, String clusterName,
			String clusterDistribution, String regressionTestName,
			String componentName, String testCaseName) {

		// Map<Integer, String> map = new HashMap<Integer, String>();
		List<ClusterData> list = new ArrayList<ClusterData>();

		// Where Clause
		String whereClause = " WHERE "
				+ getPartitions(clusterName, clusterDistribution,
						regressionTestName, componentName, testCaseName);

		// Query table
		// String sql = "select id, " + column + " from " + TABLE_NAME
		// + whereClause;

		StringBuilder sb = new StringBuilder(
		// "SELECT id, timestamp, tenantName, userId,")
				"SELECT id, timestamp, clustername, clusterdistribution, regressiontestname, componentname, testcasename, memoryutilization, vcoreutilization, cpuutilization, networkio, diskio FROM ")
				.append(TABLE_NAME).append(whereClause);

		// System.out.println("sql: " + sql);

		try {
			ResultSet res = stmt.executeQuery(sb.toString());
			while (res.next()) {

				// System.out.println("res.getInt(1)=" + res.getInt(1));
				// System.out.println("res.getString(2)=" + res.getString(2));
				// map.put(res.getInt(1), res.getString(2));

				ClusterData data = new ClusterData();
				data.setId(res.getLong("id"));
				data.setTimestamp(res.getString("timestamp"));
				// data.setTenantName(res.getString("tenantName"));
				// data.setUserId(res.getString("userId"));

				data.setClusterName(res.getString("clustername"));
				data.setClusterDistribution(res
						.getString("clusterdistribution"));
				data.setRegressionTestName(res.getString("regressiontestname"));
				data.setComponentName(res.getString("componentname"));
				data.setTestCaseName(res.getString("testcasename"));

				data.setMemoryUtilization(res.getLong("memoryutilization"));
				data.setvCoreUtilization(res.getLong("vcoreutilization"));
				data.setCpuUtilization(res.getDouble("cpuutilization"));
				// data.setNetworkIO(res.getLong("networkio"));
				// data.setDiskIO(res.getLong("diskio"));

				list.add(data);
			}
		} catch (SQLException e) {
			System.err.println("Query Data failed: " + e);
		}

		return list;
	}

	private boolean addWhereClause(StringBuilder sb, boolean firstWhereClause,
			String name, String value) {

		boolean firstClause = firstWhereClause;

		if (!CommonUtil.isNullOrEmpty(value)) {

			if (firstClause)
				firstClause = false;
			else
				sb.append(" AND ");
			sb.append(name).append(Constants.EQUAL_QUOTE).append(value)
					.append(Constants.SINGLE_QUOTE);
		}

		return firstClause;

	}

	private String getPartitions(String clusterName,
			String clusterDistribution, String regressionTestName,
			String componentName, String testCaseName) {

		StringBuilder sb = new StringBuilder();
		boolean firstClause = true;

		firstClause = addWhereClause(sb, firstClause, "ClusterName",
				clusterName);
		firstClause = addWhereClause(sb, firstClause, "ClusterDistribution",
				clusterDistribution);
		firstClause = addWhereClause(sb, firstClause, "RegressionTestName",
				regressionTestName);
		firstClause = addWhereClause(sb, firstClause, "ComponentName",
				componentName);
		firstClause = addWhereClause(sb, firstClause, "TestCaseName",
				testCaseName);

		return sb.toString();
	}

	private String getPartitionsComma(String clusterName,
			String clusterDistribution, String regressionTestName,
			String componentName, String testCaseName) {

		StringBuilder sb = new StringBuilder();
		sb.append("ClusterName='").append(clusterName)
				.append("', ClusterDistribution='").append(clusterDistribution)
				.append("', RegressionTestName='").append(regressionTestName)
				.append("', ComponentName='").append(componentName)
				.append("', TestCaseName='").append(testCaseName).append("'");

		return sb.toString();
	}

	public Integer aggregateData(String column, String clusterName,
			String clusterDistribution, String regressionTestName,
			String componentName, String testCaseName) {

		// Where Clause
		String whereClause = " WHERE "
				+ getPartitions(clusterName, clusterDistribution,
						regressionTestName, componentName, testCaseName);

		// Query table
		String sql = "select sum(" + column + ") from " + TABLE_NAME
				+ whereClause;

		System.out.println("sql=" + sql);

		int value = 0;

		try {
			ResultSet res = stmt.executeQuery(sql);
			if (res.next()) {
				value = res.getInt(1);
			}
		} catch (SQLException e) {
			System.err.println("Aggregate Data failed: " + e);
		}

		return value;
	}

	private static void usage() {
		System.out
				.println("Usage: CreateHiveTable <ClusterHostname> <REALM> <ExternalTableLocation>");
		System.out
				.println("e.g.,  CreateHiveTable aaa-cstdt-r4-n05.svr.us.AnandCompany.net NAEAST.AD.JPMORGANCHASE.COM /user/e777505");
		System.exit(1);
	}

	
	  public static void main(String[] args) throws SQLException {
	  
	  if (args.length < 3) usage();
	  
	  ConnectHiveTable createHiveTable = new ConnectHiveTable(args[0], args[1],	args[2]); 
	  
	  createHiveTable.createTable(); 
	  
	  createHiveTable.showTables(); 
	  }
	 
}
