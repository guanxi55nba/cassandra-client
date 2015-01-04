package com.example.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class SimpleClient {
	public final static String SIMPLE_STRATEGY = "SimpleStrategy";
	public final static String NETWORK_STRATEGY = "NetworkTopologyStrategy";
	public final ConsistencyLevel WRITE_CONSISTENCY = ConsistencyLevel.ONE;
	public final ConsistencyLevel READ_CONSISTENCY = ConsistencyLevel.ONE;
	

	private Cluster cluster;
	private Session session;

	private long writeStart;
	private long writeEnd;

	private long readStart;
	private long readEnd;

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect();
	}

	public void close() {
		cluster.close();
	}

	/**
	 * create keyspace demo with replication = {'class':'SimpleStrategy',
	 * 'replication_factor':2}; use demo; create table names ( id int primary
	 * key, name text ); insert into names (id,name) values (1, 'xiguan');
	 * 
	 * 
	 * @param inKeySpace
	 * @param inReplicationFactor
	 * @param inTableName
	 * @param inKeyName
	 * @param inValueName
	 */
	public void createSchema(String inKeySpace, int inReplicationFactor,
			String inTableName, String inKeyName, String inValueName) {
		StringBuilder sb = new StringBuilder();
		// Create keyspace
		sb.append("create keyspace if not exists " + inKeySpace);
		sb.append(" with replication = {'class': '" + SIMPLE_STRATEGY + "', ");
		sb.append("'replication_factor':" + inReplicationFactor + "};");

		// Create table
		sb.append("create table if not exists " + inKeySpace + "."
				+ inTableName);
		sb.append("(id int primary" + inKeyName + ", name " + inValueName + ")");

		// Execute
		session.execute(sb.toString());
	}

	public void insert(String inKeySpace, String TableName, String inKeyName,
			String inValueName, String inKey, String inValue) {
		session.execute("insert into " + inKeySpace + "." + TableName + "("
				+ inKeyName + "," + inValueName + ") " + "values (" + inKey
				+ ",'" + inValue + "');");
		;
	}

	public void createSchema(boolean inNetworkTopology) {
		if (!inNetworkTopology) {
			session.execute("create keyspace if not exists demo "
					+ "with replication = { 'class': 'SimpleStrategy', "
					+ "'replication_factor': 6 };");
			session.execute("create table if not exists demo.name ("
					+ "id int primary key, name text);");
		} else {
			session.execute("create keyspace if not exists demo "
					+ "with replication = { 'class' : 'NetworkTopologyStrategy', 'DC1' : 2, 'DC2' : 2, 'DC3': 2};");
			session.execute("create table if not exists demo.name ("
					+ "id int primary key, name text);");
		}
	}

	public void loadData(boolean inNetworkTopology) {

		if (!inNetworkTopology) {
			session.execute("insert into demo.name "
					+ "(id, name) values (1,'xiguan');");
			session.execute("insert into demo.name "
					+ "(id, name) values (2,'qinjin');");
			session.execute("insert into demo.name "
					+ "(id, name) values (3,'yingliu');");
		} else {

			writeStart = System.currentTimeMillis();

			String[] firstKeys = { "id", "name" };
			Object[] firstValues = { 1, "xiguan" };
			addDataInNetworkTopology(firstKeys, firstValues);

			String[] secondKeys = { "id", "name" };
			Object[] secondValues = { 2, "qinjin" };
			addDataInNetworkTopology(secondKeys, secondValues);

			String[] thirdKeys = { "id", "name" };
			Object[] thirdValues = { 3, "yingliu" };

			writeEnd = System.currentTimeMillis();

			addDataInNetworkTopology(thirdKeys, thirdValues);
		}
	}

	private void addDataInNetworkTopology(String[] inKeys, Object[] inValues) {
		Statement statement = QueryBuilder.insertInto("demo", "name")
				.values(inKeys, inValues)
				.setConsistencyLevel(WRITE_CONSISTENCY);
		session.execute(statement);
	}

	public void printResults(boolean inNetworkTopology) {

		if (!inNetworkTopology) {
			ResultSet results = session.execute("SELECT * FROM "
					+ "demo.name ;");
			for (Row row : results) {
				System.out.println("Id: " + row.getInt("id") + ", " + "name: "
						+ row.getString("name"));
			}
		} else {

			Statement statement = QueryBuilder.select().all()
					.from("demo", "name")
					.setConsistencyLevel(READ_CONSISTENCY);
			readStart = System.currentTimeMillis();
			ResultSet results = session.execute(statement);
			readEnd = System.currentTimeMillis();
			for (Row row : results) {
				System.out.println("Id: " + row.getInt("id") + ", " + "name: "
						+ row.getString("name"));
			}
			System.out.println("Write 3 values, duration: "
					+ (writeEnd - writeStart));
			System.out.println("Read 3 values, duration: "
					+ (readEnd - readStart));
		}
	}

	public static void main(String[] args) {
		SimpleClient client = new SimpleClient();
		boolean m_newtworkStrategy = true;
		client.connect("172.17.0.2");
		client.createSchema(m_newtworkStrategy);
		client.loadData(m_newtworkStrategy);
		client.printResults(m_newtworkStrategy);
		client.close();
	}
}
