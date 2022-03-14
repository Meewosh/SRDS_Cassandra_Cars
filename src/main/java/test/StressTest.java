package test;
import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.UUID;


public class StressTest extends Thread {
    BackendSession session;

    public StressTest(BackendSession session) {
        this.session = session;
    }
    public static final Logger logger = LoggerFactory.getLogger(StressTest.class);

    @Override
    public void run() {
        UUID userId = UUID.randomUUID();
        UUID rs_id = UUID.randomUUID();

        String carBrand = "Renualt";
        String carModel = "Capture 1.0 TCe Zen";

        try {
            String rs = session.selectConcreteCarAndCheckAvailability(carBrand, carModel, userId);
            System.out.println(rs + "\n");
        } catch (BackendException e) {
            e.printStackTrace();
        }


//        Random rand = new Random();
//        int n = rand.nextInt(6);
//        switch (n) {
//            case 0: {
//                String registrationNumber = "LLE5929";
//                String model = "GLE Coupe 350 d 4-Matic";
//                String brand = "Mercedes-Benz";
//                try {
//                    session.upsertReservation(registrationNumber, model, brand, rs_id, userId);
//                    List<Row> Car = session.selectCarReservationForUser(userId);
//
//                    if(Car.size() == 0)
//                    {
//                        logger.info("WARNING");
//                        System.out.println("Brak rezerwacji dla auta " + brand + " " + model + " dla uzytkowanika z ID: " + userId);
//                    }
//                } catch (BackendException e) {
//                    e.printStackTrace();
//                }
//                break;
//            }
//            case 1: {
//                String registrationNumber = "CIN1938";
//                String model = "A110";
//                String brand = "Alpine";
//                try {
//                    session.upsertReservation(registrationNumber, model, brand, rs_id, userId);
//                    List<Row> Car = session.selectCarReservationForUser(userId);
//
//                    if(Car.size() == 0)
//                    {
//                        logger.info("WARNING");
//                        System.out.println("Brak rezerwacji dla auta " + brand + " " + model + " dla uzytkowanika z ID: " + userId);
//
//                    }
//
//                } catch (BackendException e) {
//                    e.printStackTrace();
//                }
//                break;
//            }
//            case 2: {
//                String registrationNumber = "WX91806";
//                String model = "Seria 5 530d";
//                String brand = "BMW";
//                try {
//                    session.upsertReservation(registrationNumber, model, brand, rs_id, userId);
//                    List<Row> Car = session.selectCarReservationForUser(userId);
//
//                    if(Car.size() == 0)
//                    {
//                        logger.info("WARNING");
//                        System.out.println("Brak rezerwacji dla auta " + brand + " " + model + " dla uzytkowanika z ID: " + userId);
//
//                    }
//                } catch (BackendException e) {
//                    e.printStackTrace();
//                }
//                break;
//            }
//            case 3: {
//                String registrationNumber = "NOL9272";
//                String model = "Capture 1.0 TCe Zen";
//                String brand = "Renualt";
//                try {
//                    session.upsertReservation(registrationNumber, model, brand, rs_id, userId);
//                    List<Row> Car = session.selectCarReservationForUser(userId);
//
//                    if(Car.size() == 0)
//                    {
//                        logger.info("WARNING");
//                        System.out.println("Brak rezerwacji dla auta " + brand + " " + model + " dla uzytkowanika z ID: " + userId);
//
//                    }
//                } catch (BackendException e) {
//                    e.printStackTrace();
//                }
//                break;
//            }
//            case 4: {
//                String registrationNumber = "SCZ0047";
//                String model = "Mustang 5.0 V8 GT";
//                String brand = "Ford";
//                try {
//                    session.upsertReservation(registrationNumber, model, brand, rs_id, userId);
//                    List<Row> Car = session.selectCarReservationForUser(userId);
//
//                    if(Car.size() == 0)
//                    {
//                        logger.info("WARNING");
//                        System.out.println("Brak rezerwacji dla auta " + brand + " " + model + " dla uzytkowanika z ID: " + userId);
//                    }
//                } catch (BackendException e) {
//                    e.printStackTrace();
//                }
//                break;
//            }
//            case 5: {
//                String registrationNumber = "PKL9977";
//                String model = "Diablo";
//                String brand = "Lamborghini";
//                try {
//                    session.upsertReservation(registrationNumber, model, brand, rs_id, userId);
//                    List<Row> Car = session.selectCarReservationForUser(userId);
//
//                    if(Car.size() == 0)
//                    {
//                        logger.info("WARNING");
//                        System.out.println("Brak rezerwacji dla auta " + brand + " " + model + " dla uzytkowanika z ID: " + userId);
//
//                    }
//                } catch (BackendException e) {
//                    e.printStackTrace();
//                }
//                break;
//
//            }
//            default:
//                break;
//        }

    }
}
