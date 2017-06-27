package ch.furthermore.poc.idgen;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.jws.WebMethod;
import javax.jws.WebService;

/**
 * <pre>
 * mvn clean install
 * mv target/id-gen-poc.war ~/Java/wildfly-10.1.0.Final/standalone/deployments/
 * curl http://localhost:8080/id-gen-poc/IDGenerator?wsdl
 * </pre>
 * 
 * <pre>
 * Deploy in WLS 12.2 console (all defaults)
 * curl http://172.16.210.129:7003/IDGenerator/IDGeneratorService?WSDL
 * </pre>
 */
@Stateless
@WebService
public class IDGenerator {
	private final static Logger logger = Logger.getLogger(IDGenerator.class.getName()); 
	private final static Object monitor = new Object();

	@PostConstruct
	public void initOnce() {
		try {
			Class.forName("org.h2.Driver");

			try (Connection conn = connection()) {
				try (Statement s = conn.createStatement()) {
					s.execute("create table CATEGORY(name varchar(255) PRIMARY KEY, value int not null)");
					
					logger.info("Table CATEGORY created");
				}
				catch (SQLException e) {
					//ignore "Table already exists" exception
					
					logger.info("Table CATEGORY already exists");
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@WebMethod
	public String generate(String category) {
		synchronized (monitor) {
			try (Connection conn = connection()) {
				Integer currentValue = null;
				try (PreparedStatement ps = conn.prepareStatement("select value from CATEGORY where name=?")) {
					ps.setString(1, category);
					try (ResultSet r = ps.executeQuery()) {
						if (r.next()) {
							currentValue = r.getInt(1);
						}
					}
				}
				if (currentValue == null) {
					currentValue = 0;
					try (PreparedStatement ps = conn
							.prepareStatement("insert into CATEGORY(name,value) values (?,?)")) {
						ps.setString(1, category);
						ps.setInt(2, currentValue);
						ps.execute();
					}
				} else {
					currentValue = currentValue + 1;
					try (PreparedStatement ps = conn.prepareStatement("update CATEGORY set value=? where name=?")) {
						ps.setInt(1, currentValue);
						ps.setString(2, category);
						ps.execute();
					}
				}
				
				String result = category + "-" + currentValue;
				
				logger.info("New ID: " + result);
				
				return result;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private Connection connection() throws SQLException {
		return DriverManager.getConnection("jdbc:h2:~/idgenpoc");
	}
}
