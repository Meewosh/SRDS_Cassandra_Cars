package cassdemo;

import java.io.IOException;
import java.util.Properties;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.Row;
import test.StressTest;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

	private static final String PROPERTIES_FILENAME = "config.properties";

	public static void main(String[] args) throws IOException, BackendException {
		String contactPoint = null;
		String keyspace = null;

		Scanner input = new Scanner(System.in);

		Properties properties = new Properties();
		try {
			properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

			contactPoint = properties.getProperty("contact_point");
			keyspace = properties.getProperty("keyspace");
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		BackendSession session = new BackendSession(contactPoint, keyspace);



		while(true){
			System.out.println("\n0 - zamkniecie programu oraz wyczyszczenie tabeli Reservation_Cars");
			System.out.println("1 - Auta");
			System.out.println("2 - Stress Test");
			System.out.println("3 - Wyczyszczenie tablei Reservation_Cars");
			System.out.println("4 - Tabela Reservation_Cars");
			System.out.println("5 - Transfer Rejstracji do tabeli Car Status");


			String mode = input.next();

			switch (mode){
				case "0":
				{
//					session.deleteAll();
					System.exit(0);
				}
				case "1":
				{
					String output = session.selectAll();
					System.out.println("Cars: \n" + output);
						break;
				}
				case "2":
				{
					ExecutorService executorService = Executors.newFixedThreadPool(15);
					for (int i = 0; i < 15; i++) {
						executorService.execute(new Thread(new StressTest(session)));
					}

					break;
				}
				case "3":
				{
					session.deleteAll();

					break;
				}
				case "4":
				{
					String output = session.selectAllCarReservation();
					System.out.println("Cars: \n" + output);

					break;
				}
				case "5":
				{
					session.insertCarRegistrationToCarStatus();
					System.out.println("Done\n");

				}
				default:
				{
					System.out.println("Prosimy o wybranie poprawnej opcji!\n");
						break;
				}
			}
		}
	}
}
