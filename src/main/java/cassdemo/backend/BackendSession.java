package cassdemo.backend;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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

	private static PreparedStatement DELETE_ALL_FROM_CARS;
	private static PreparedStatement DELETE_ALL_FROM_RESERVATION_CARS;
	private static PreparedStatement DELETE_ALL_FROM_RESERVATION_CARS_BY_USERS;

	private static PreparedStatement SELECT_ALL_FROM_CARS;
	private static PreparedStatement SELECT_ALL_CARS_BY_MODEL_AND_BRAND;
	private static PreparedStatement SELECT_CONCRETE_CAR_RESERVATION_BY_REGISTRATION_NUMBER;

	private static PreparedStatement INSERT_INTO_CARS;
	private static PreparedStatement INSERT_INTO_CARS_RESERVATION;
	private static PreparedStatement INSERT_INTO_CARS_RESERVATION_BY_USER;

	private static final String CARS_FORMAT = "- %-10s  %-30s %-15s %-10s %-10s\n";

	private volatile int counter;
	private volatile int counterRead;
	private volatile int counterWrite;


	private void prepareStatements() throws BackendException {
		try {

			DELETE_ALL_FROM_CARS = session.prepare("TRUNCATE cars;");
			DELETE_ALL_FROM_RESERVATION_CARS = session.prepare("TRUNCATE Reservation_Cars;");
			DELETE_ALL_FROM_RESERVATION_CARS_BY_USERS = session.prepare("TRUNCATE reservation_by_user;");

			SELECT_ALL_FROM_CARS = session.prepare("SELECT * FROM Cars");
			SELECT_ALL_CARS_BY_MODEL_AND_BRAND = session.prepare("SELECT * FROM Cars WHERE model = (?) AND brand = (?)");
			SELECT_CONCRETE_CAR_RESERVATION_BY_REGISTRATION_NUMBER = session.prepare("SELECT * FROM Reservation_Cars WHERE registrationnumber = (?) AND day = (?)");

			INSERT_INTO_CARS_RESERVATION_BY_USER = session.prepare("INSERT INTO Reservation_by_user(rs_id, registrationNumber, user_id, date, brand, model) VALUES (?, ?, ?, ?, ?, ?)");
			INSERT_INTO_CARS_RESERVATION = session.prepare("INSERT INTO Reservation_Cars(rs_id, user_id, registrationNumber, day) VALUES (?, ?, ?, ?)");
			INSERT_INTO_CARS = session.prepare("INSERT INTO Cars(registrationNumber, model, brand, productionYear, color) VALUES(?, ?, ?, ?, ?)");

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

			builder.append(String.format(CARS_FORMAT, registrationNumber, model, brand, productionYear, color));
		}
		return builder.toString();
	}

	//wybranie konkretnego auta
	public ResultSet selectConcreteCar(String carBrand, String carModel) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_CARS_BY_MODEL_AND_BRAND);
		bs.bind(carModel, carBrand);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
			synchronized (this){
				counter += 1;
				counterRead += 1;
			}
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs;
	}

	//sprawdzenie czy auto o podanej rejstracj i dacie jest zarezerwowane
	//jezeli rekord jest w bazie to jest zarezerwowane
	public boolean isCarReservedForDate(String registrationNumber, String date) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_CONCRETE_CAR_RESERVATION_BY_REGISTRATION_NUMBER);
		bs.bind(registrationNumber, date);
		ResultSet rs = null;
		//boolean isReserved = false;
		try {
			rs = session.execute(bs);
			synchronized (this){
				counter += 1;
				counterRead += 1;
			}

			if (rs.one() != null) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
	}

	//sprawdzenie czy auto zostalo dodane do bazy Reservation_Cars z poprawym rs_id oraz user_id
	public boolean getSpecificCarReservation(String registrationNumber, String date, UUID reservationId, UUID userId) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_CONCRETE_CAR_RESERVATION_BY_REGISTRATION_NUMBER);
		bs.bind(registrationNumber, date);

		ResultSet rs = null;
		try {
			rs = session.execute(bs);
			synchronized (this){
				counter += 1;
				counterRead += 1;
			}
			List<Row> allRows = rs.all();
			if (allRows.size() > 0) {
				Row row = allRows.get(allRows.size() - 1);
				if ((row.getUUID("rs_id").compareTo(reservationId) == 0) && (row.getUUID("user_id").compareTo(userId) == 0)){
					return true;
				} else {
					logger.error("UserID in DB: " + row.getUUID("user_id") + "User expected: " + userId +
							"ReservationID in DB: " + row.getUUID("rs_id") + "ReservationID expected: "+ reservationId);
					return false;
				}
			} else {
				logger.warn("Null in DB");
				return false;
			}

		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
	}

	//wprowadzenie rezerwacji do tebeli Resrvation_Cars
	public void insertIntoCarsReservation(UUID rs_id, UUID user_id, String registrationNumber, String date) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_CARS_RESERVATION);
		bs.bind(rs_id, user_id, registrationNumber, date);

		try {
			session.execute(bs);
			synchronized (this){
				counter += 1;
				counterWrite += 1;
			}

		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
	}

	//wprowadzenie rekordu do tabeli Reservation_by_user
	public void insertIntoReservationByUser(UUID rs_id, String registrationNumber, UUID user_id, String date, String brand, String model) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_CARS_RESERVATION_BY_USER);
		bs.bind(rs_id, registrationNumber, user_id, date, brand, model);

		try {
			session.execute(bs);
			synchronized (this){
				counter += 1;
				counterWrite += 1;
			}
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
	}

	//wprowadzenie auta do baz danych cars i cars_by_registration_number
	public void insertCar(String registrationNumber, String model, String brand, String productionYear, String color) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_CARS);
		bs.bind(registrationNumber, model, brand, productionYear, color);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}
	}

	//usuniecie wszystkich wpisow z bazy
	public void deleteAll() throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FROM_RESERVATION_CARS_BY_USERS);
		BoundStatement bs1 = new BoundStatement(DELETE_ALL_FROM_CARS);
		BoundStatement bs2 = new BoundStatement(DELETE_ALL_FROM_RESERVATION_CARS);
		try {
			session.execute(bs);
			session.execute(bs1);
			session.execute(bs2);

		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
		}
		logger.info("All cars reservation deleted");
	}

	//stworzenie nowego auta
	public void createCar(int amountOfCars) throws BackendException {
		for (int i = 0; i < amountOfCars; i++) {
			String[] carTableBrand = {"Mercedes-Benz", "BMW", "Renualt", "Lamborghini", "Ford", "Volkswagen", "Audi", "Hyundai", "Kia"};
			String[] carTableModel = {"1050e small", "1586w big", "6947aa medium", "123", "1", "332w", "Speed", "Off-road", "Electric"};
			String[] color = {"black", "red", "white", "grey", "yellow", "blue"};
			Random rand = new Random();

			int randomNumberOfCarBrand = rand.nextInt(9);
			int randomNumberOfCarModel = rand.nextInt(9);

			String c = String.valueOf((char) ('A'));
			String registrationNumber = null;

			if (i < 10){
				registrationNumber = c + 0 + 0 + 0 + 0 + i;
			} else if(i < 100) {
				registrationNumber = c + 0 + 0 + 0 + i;
			} else if( i < 1000) {
				registrationNumber = c + 0 + 0 + i;
			} else if( i < 10000) {
				registrationNumber = c + 0 + i;
			} else if( i < 100000) {
				registrationNumber = c + i;
			}

			int low = 2000;
			int high = 2022;
			String productionYear = String.valueOf(rand.nextInt(high - low) + low);
			int randomNumberOfColor = rand.nextInt(6);

			insertCar(registrationNumber,
					carTableModel[randomNumberOfCarModel],
					carTableBrand[randomNumberOfCarBrand],
					productionYear,
					color[randomNumberOfColor]
			);
			//logger.info("Dodano auto: " + carTableBrand[randomNumberOfCarBrand] + " " + carTableModel[randomNumberOfCarModel] + " " + registrationNumber);
		}
	}

	public void showCounter(){
		System.out.println("Po stress tescie: ");
		System.out.println("Liczba odczytow i zapisow: " + counter);
		System.out.println("Liczba odczytow: " + counterRead);
		System.out.println("Liczba zapisow: " + counterWrite);
	}

	public void setCounterToDefault(){
		counter = 0;
		counterRead = 0;
		counterWrite = 0;
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