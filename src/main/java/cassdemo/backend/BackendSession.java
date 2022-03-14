package cassdemo.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;
import java.util.UUID;

import static com.datastax.driver.core.ConsistencyLevel.ONE;

/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	public static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private static PreparedStatement DELETE_ALL_FROM_CARS_RESERVATION;
	private static PreparedStatement SELECT_ALL_FROM_CARS;
	private static PreparedStatement SELECT_ALL_FROM_CARS_RESERVATION;
	private static PreparedStatement INSERT_INTO_CARS;
	private static PreparedStatement INSERT_INTO_CARS_STATUS_INIT;
	private static PreparedStatement SELECT_CONCRETE_USER_FROM_CAR_RESERVATION;
	private static PreparedStatement SELECT_ALL_AVAILABLE_CARS;
	private static PreparedStatement SELECT_CONCRETE_CAR_STATUS;
	private static PreparedStatement UPDATE_CAR_AVAILABILITY;



	private static final String REGISTRATION_NUMBER_FORMAT = "- %-10s\n";
	private static final String CARS_FORMAT = "- %-10s  %-30s %-15s %-10s %-10s\n";
	private static final String CARS_RESERVATION_FORMAT = "- %-40s  %-10s %-25s %-15s %-40s\n";



	private void prepareStatements() throws BackendException {
		try {
			DELETE_ALL_FROM_CARS_RESERVATION = session.prepare("TRUNCATE Reservation_Cars;");
			SELECT_ALL_FROM_CARS = session.prepare("SELECT * FROM Cars");
			SELECT_ALL_AVAILABLE_CARS = session.prepare("SELECT * FROM Cars WHERE model = (?) AND brand = (?)");
			SELECT_CONCRETE_CAR_STATUS = session.prepare("SELECT * FROM car_status WHERE registrationNumber = (?)");
			UPDATE_CAR_AVAILABILITY = session.prepare("UPDATE car_status SET status = (?), user_id = (?) WHERE registrationNumber = (?)");
			INSERT_INTO_CARS = session
					.prepare("INSERT INTO Reservation_Cars(rs_id, registrationNumber, model, brand, user_id) VALUES (?, ?, ?, ?, ?)");
			INSERT_INTO_CARS_STATUS_INIT = session.prepare("INSERT INTO car_status(registrationNumber, status, user_id) VALUES(?, ?, ?)");
			SELECT_CONCRETE_USER_FROM_CAR_RESERVATION = session.prepare("SELECT * FROM Reservation_Cars where user_id = (?)").setConsistencyLevel(ONE);
			SELECT_ALL_FROM_CARS_RESERVATION = session.prepare("SELECT * FROM Reservation_Cars");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public String selectAll() throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_CARS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			String registrationNumber = row.getString("registrationNumber");
			String model = row.getString("model");
			String brand = row.getString("brand");
			String productionYear = row.getString("productionYear");
			String color = row.getString("color");

			builder.append(String.format(CARS_FORMAT, registrationNumber, model, brand, productionYear,color));
		}

		return builder.toString();
	}

	public String selectConcreteCarAndCheckAvailability(String carBrand, String carModel, UUID userID) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_AVAILABLE_CARS);
		bs.bind(carModel, carBrand);

		ResultSet rs = null;

		logger.info("dupa1");

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		logger.info(rs.one().toString());


		for (Row row : rs) {
			String registrationNumber = row.getString("registrationNumber");

			boolean carAvailability = isAvailable(registrationNumber);

			logger.info("Car avail.: " + carAvailability);

			if (carAvailability) {
				builder.append(String.format(REGISTRATION_NUMBER_FORMAT, registrationNumber));
				updateCarAvailability(registrationNumber, userID);
				logger.info("wolne auto, zmieniam na false" + registrationNumber);
				return builder.toString();
			}
		}
		logger.info("no Car");
		return "There is no available car.";
	}

	public boolean isAvailable(String registrationNumber) throws BackendException {

		BoundStatement bs = new BoundStatement(SELECT_CONCRETE_CAR_STATUS);
		bs.bind(registrationNumber);
		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		return rs.one().getBool("status");
	}

	public void updateCarAvailability(String registrationNumber, UUID userID) throws BackendException {

		BoundStatement bs = new BoundStatement(UPDATE_CAR_AVAILABILITY);
		bs.bind(false, userID, registrationNumber);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

	}

	public String selectAllCarReservation() throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_CARS_RESERVATION);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			UUID rs_id = row.getUUID("rs_id");
			String registrationNumber = row.getString("registrationNumber");
			String model = row.getString("model");
			String brand = row.getString("brand");
			UUID user_id = row.getUUID("user_id");

			builder.append(String.format(CARS_RESERVATION_FORMAT, rs_id , registrationNumber, model, brand, user_id));
		}

		return builder.toString();
	}

	public List<Row> selectCarReservationForUser(UUID userId) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_CONCRETE_USER_FROM_CAR_RESERVATION);
		bs.bind(userId);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		return rs.all();
	}

	public List<Row> selectCars() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_CARS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		return rs.all();
	}

	public void upsertReservation(String registrationNumber, String model, String brand, UUID rs_id, UUID user_id) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_CARS);
		bs.bind(rs_id, registrationNumber, model, brand, user_id);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Car " + brand + " " + model + " upserted for " + user_id);
	}

	public void deleteAll() throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FROM_CARS_RESERVATION);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
		}

		logger.info("All cars reservation deleted");
	}

	public void insertCarRegistrationToCarStatus() throws BackendException {

		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_CARS);
		BoundStatement insertIntoCarStatus = new BoundStatement(INSERT_INTO_CARS_STATUS_INIT);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			String registrationNumber = row.getString("registrationNumber");
			insertIntoCarStatus.bind(registrationNumber, true);
			try {
				session.execute(insertIntoCarStatus);
			} catch (Exception e) {
				throw new BackendException("dupa " + e.getMessage() + ".", e);
			}
		}
	}

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}



}
