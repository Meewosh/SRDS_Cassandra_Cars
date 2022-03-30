package cassdemo.backend;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

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

	//private static PreparedStatement DELETE_ALL_FROM_CARS_RESERVATION;
	private static PreparedStatement DELETE_ALL_FROM_CARS;
	private static PreparedStatement DELETE_ALL_FROM_CARS_BY_REGISTRATION_NUMBER;
	private static PreparedStatement DELETE_ALL_FROM_RESERVATION_CARS;
	private static PreparedStatement DELETE_ALL_FROM_RESERVATION_CARS_BY_USERS;



	private static PreparedStatement SELECT_ALL_FROM_CARS;

	private static PreparedStatement INSERT_INTO_CARS;
	private static PreparedStatement INSERT_INTO_CARS_REGISTRATION_NUMBER;
	private static PreparedStatement INSERT_INTO_CARS_STATUS_INIT;

	private static PreparedStatement SELECT_ALL_AVAILABLE_CARS_BY_MODEL_AND_BRAND;
	private static PreparedStatement SELECT_CONCRETE_CAR_RESERVATION_BY_REGISTRATION_NUMBER;
	private static PreparedStatement UPDATE_CAR_RESERVATION;
	private static PreparedStatement SELECT_CARS_BY_REGISTRATION_NUMBER;
	private static PreparedStatement SELECT_CONCRETE_CAR_BY_REGISTRATION_NUMBER;
	private static PreparedStatement INSERT_INTO_CARS_RESERVATION;
	private static PreparedStatement DELETE_ROW_FROM_CAR_RESERVATION;
	private static PreparedStatement SELECT_FROM_CAR_RESERVATION_BY_USER;
	private static PreparedStatement SELECT_FROM_CAR_RESERVATION_BY_USER_AND_RESERVATION;
	private static PreparedStatement INSERT_INTO_CARS_RESERVATION_BY_USER;


	private static final String CARS_FORMAT = "- %-10s  %-30s %-15s %-10s %-10s\n";

	private void prepareStatements() throws BackendException {
		try {
			//DELETE_ALL_FROM_CARS_RESERVATION = session.prepare("TRUNCATE car_status;");
			DELETE_ALL_FROM_CARS = session.prepare("TRUNCATE cars;");
			DELETE_ALL_FROM_CARS_BY_REGISTRATION_NUMBER= session.prepare("TRUNCATE cars_by_registrationnumber;");
			DELETE_ALL_FROM_RESERVATION_CARS = session.prepare("TRUNCATE Reservation_Cars;");
			DELETE_ALL_FROM_RESERVATION_CARS_BY_USERS = session.prepare("TRUNCATE reservation_by_user;");

			SELECT_ALL_FROM_CARS = session.prepare("SELECT * FROM Cars");
			SELECT_ALL_AVAILABLE_CARS_BY_MODEL_AND_BRAND = session.prepare("SELECT * FROM Cars WHERE model = (?) AND brand = (?)");
			SELECT_CONCRETE_CAR_BY_REGISTRATION_NUMBER = session.prepare("SELECT * FROM cars_by_registrationnumber WHERE registrationnumber = (?)");
			SELECT_CONCRETE_CAR_RESERVATION_BY_REGISTRATION_NUMBER = session.prepare("SELECT * FROM Reservation_Cars WHERE registrationnumber = (?)");
			SELECT_FROM_CAR_RESERVATION_BY_USER = session.prepare("SELECT * FROM Reservation_by_user WHERE user_id = (?)");
			SELECT_FROM_CAR_RESERVATION_BY_USER_AND_RESERVATION = session.prepare("SELECT * FROM Reservation_by_user WHERE user_id = (?) and rs_id = (?)");

			UPDATE_CAR_RESERVATION = session.prepare("UPDATE Reservation_Cars SET user_id = (?), day = (?), rs_id  = (?) WHERE registrationnumber = (?)");
			DELETE_ROW_FROM_CAR_RESERVATION = session.prepare("DELETE FROM Reservation_Cars WHERE registrationNumber = (?)");

			INSERT_INTO_CARS_RESERVATION_BY_USER = session.prepare("INSERT INTO Reservation_by_user(rs_id, registrationNumber, user_id, dateFrom, dateTo, brand, model) VALUES (?, ?, ?, ?, ?, ?, ?)");
			INSERT_INTO_CARS_RESERVATION = session.prepare("INSERT INTO Reservation_Cars(rs_id, user_id, registrationNumber, day) VALUES (?, ?, ?, ?)");
//			INSERT_INTO_CARS_STATUS_INIT = session.prepare("INSERT INTO car_status(registrationNumber, status, user_id) VALUES(?, ?, ?)");
			INSERT_INTO_CARS = session.prepare("INSERT INTO Cars(registrationNumber, model, brand, productionYear, color) VALUES(?, ?, ?, ?, ?)");
			INSERT_INTO_CARS_REGISTRATION_NUMBER = session.prepare("INSERT INTO cars_by_registrationnumber(registrationNumber, model, brand, productionYear, color) VALUES(?, ?, ?, ?, ?)");

		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	//wybranie wszytskich aut
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

	public ResultSet selectConcreteCarAndCheckAvailability(String carBrand, String carModel) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_AVAILABLE_CARS_BY_MODEL_AND_BRAND);
		bs.bind(carModel, carBrand);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		return rs;
	}

	public ResultSet selectCarReservationForConcreteUser(UUID user_id) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_FROM_CAR_RESERVATION_BY_USER);
		bs.bind(user_id);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		return rs;
	}

	public String selectConcreteReservationByUserIdAndReservationId(UUID user_id, UUID rs_id) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_FROM_CAR_RESERVATION_BY_USER_AND_RESERVATION);
		bs.bind(user_id, rs_id);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		return rs.one().getString("registrationnumber");
	}


	public boolean carRegistrationNumber(String registrationNumber) throws BackendException {

		BoundStatement bs = new BoundStatement(SELECT_CONCRETE_CAR_BY_REGISTRATION_NUMBER);
		bs.bind(registrationNumber);
		ResultSet rs = null;

		try {
			rs = session.execute(bs);
			if (rs.all().size() == 0) return true;
			else return false;

		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
	}


	//sprawdzenie dostepnosci auta w tabeli cars_status
	public boolean isDefaultDate(String registrationNumber) throws BackendException {

		BoundStatement bs = new BoundStatement(SELECT_CONCRETE_CAR_RESERVATION_BY_REGISTRATION_NUMBER);
		bs.bind(registrationNumber);
		ResultSet rs = null;
		boolean isDefault = false;
		try {
			rs = session.execute(bs);
			if (Objects.equals(rs.one().getString("day"), "1900-01-01")) isDefault = true;
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		return isDefault;
	}

	//sprawdzenie czy uzytkownik wprowadzony do bazy danych jest identyczny z poleceniem
	public UUID getUserIDbyCarRegistrationNumber(String registrationNumber) throws BackendException {

		BoundStatement bs = new BoundStatement(SELECT_CONCRETE_CAR_RESERVATION_BY_REGISTRATION_NUMBER);
		bs.bind(registrationNumber);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		return rs.one().getUUID("user_id");
	}


	public boolean updateCarReservation(String registrationNumber, UUID userID, UUID rs_id, String day) throws BackendException {

		BoundStatement bs = new BoundStatement(UPDATE_CAR_RESERVATION);
		bs.bind(userID, day, rs_id, registrationNumber);
		try {
			if (isDefaultDate(registrationNumber)) {
				session.execute(bs);
				return true;
			}
			else {
				return false;
			}
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

	}

	public void insertIntoCarsReservation(UUID rs_id, UUID user_id, String registrationNumber, String day) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_CARS_RESERVATION);
		bs.bind(rs_id, user_id, registrationNumber, day);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

	}


	public void insertIntoReservationByUser(UUID rs_id, String registrationNumber, UUID user_id,  String dateFrom, String dateTo, String brand, String model) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_CARS_RESERVATION_BY_USER);
		bs.bind(rs_id, registrationNumber, user_id, dateFrom, dateTo, brand, model);
		
		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

	}

	//wprowadzenie auta do baz danych cars i cars_by_registration_number
	public void upsertCar(String registrationNumber, String model, String brand, String productionYear, String color) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_CARS);
		BoundStatement bs1 = new BoundStatement(INSERT_INTO_CARS_REGISTRATION_NUMBER);

		bs.bind(registrationNumber, model, brand, productionYear, color);
		bs1.bind(registrationNumber, model, brand, productionYear, color);

		try {
			session.execute(bs);
			session.execute(bs1);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}
	}


	//usuniecie wszystkich wpisow z bazy
	public void deleteAll() throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FROM_RESERVATION_CARS_BY_USERS);
		BoundStatement bs1 = new BoundStatement(DELETE_ALL_FROM_CARS);
		BoundStatement bs2 = new BoundStatement(DELETE_ALL_FROM_CARS_BY_REGISTRATION_NUMBER);
		BoundStatement bs3 = new BoundStatement(DELETE_ALL_FROM_RESERVATION_CARS);
		try {
			session.execute(bs);
			session.execute(bs1);
			session.execute(bs2);
			session.execute(bs3);

		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
		}

		logger.info("All cars reservation deleted");
	}

	public void deleteRowFromCarReservation(UUID user_id, UUID rs_id) throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_ROW_FROM_CAR_RESERVATION);
		bs.bind(user_id, rs_id);

		try {
			session.execute(bs);
			logger.info("usunieto wpis z rs_id: " + rs_id + " oraz user_id: " + user_id);
		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
		}
	}


	//wprowadzenie rekordow do bazy cars_status
	public void insertCarRegistrationToCarStatus(String registrationNumber) throws BackendException {

		BoundStatement insertIntoCarStatus = new BoundStatement(INSERT_INTO_CARS_STATUS_INIT);
		insertIntoCarStatus.bind(registrationNumber, true, null);
		ResultSet rs = null;

		try {
			rs = session.execute(insertIntoCarStatus);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

	}

	//generacja numerow rejstracyjnych
	public String registrationNumberGeneration() throws BackendException {
		String registrationNumber;
		Random rnd = new Random();


		while(true){
			String c = String.valueOf((char) ('A' + rnd.nextInt(26)));
			String c1 = String.valueOf((char) ('A' + rnd.nextInt(26)));
			String c2 = String.valueOf((char) ('A' + rnd.nextInt(26)));
			String number = String.valueOf(rnd.nextInt(10));
			String number1 = String.valueOf(rnd.nextInt(10));
			String number2 = String.valueOf(rnd.nextInt(10));
			String number3 = String.valueOf(rnd.nextInt(10));

			registrationNumber = c + c1 + c2 + number + number1 + number2 + number3;
			//List<Row> car = selectCars(registrationNumber);


			if (carRegistrationNumber(registrationNumber)){
				return registrationNumber;
			}
			logger.info("W bazie danych znaleziono takie samo auto");
		}

	}


	//stworzenie nowego auta
	public void createCar() throws BackendException {
		String[] carTableBrand = {"Mercedes-Benz", "BMW", "Renualt", "Lamborghini", "Ford", "Volkswagen","Audi","Hyundai","Kia"};
		String[] carTableModel = {"1050e small", "1586w big", "6947aa medium", "123", "1", "332w","Speed","Off-road","Electric"};
		String[] color = {"black", "red", "white", "grey", "yellow", "blue"};
        Random rand1 = new Random();
		Random rand2 = new Random();
		Random rand3 = new Random();
		Random rand4 = new Random();

		int randomNumberOfCarBrand = rand1.nextInt(9);
		int randomNumberOfCarModel = rand2.nextInt(9);

		String registrationNumber = registrationNumberGeneration();

		int low = 2000;
		int high = 2022;
		String productionYear = String.valueOf(rand3.nextInt(high-low) + low);
		int randomNumberOfColor = rand4.nextInt(6);

		upsertCar(registrationNumber,
				carTableModel[randomNumberOfCarModel],
				carTableBrand[randomNumberOfCarBrand],
				productionYear,
				color[randomNumberOfColor]
				);

		insertIntoCarsReservation(null, null, registrationNumber, "1900-01-01");


		logger.info("Dodano auto: " + carTableBrand[randomNumberOfCarBrand] + " " + carTableModel[randomNumberOfCarModel] + " " + registrationNumber);

//		insertCarRegistrationToCarStatus(registrationNumber);
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
