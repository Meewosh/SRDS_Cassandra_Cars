package test;
import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.ResultSet;
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

        String[] carTableBrand = {"Mercedes-Benz", "BMW", "Renualt", "Lamborghini", "Ford", "Volkswagen","Audi","Hyundai","Kia"};
        String[] carTableModel = {"1050e small", "1586w big", "6947aa medium", "123", "1", "332w","Speed","Off-road","Electric"};
        Random rand1 = new Random();
        Random rand2 = new Random();
        int randomNumberOfCarBrand = rand1.nextInt(9);
        int randomNumberOfCarModel = rand2.nextInt(9);

        ResultSet queryAll = null;

        //wynajecie auta
            try {
                queryAll = session.selectConcreteCarAndCheckAvailability(carTableBrand[randomNumberOfCarBrand], carTableModel[randomNumberOfCarModel], userId);
                for (Row row : queryAll) {
                    String registrationNumber = row.getString("registrationNumber");

                    boolean carAvailability = session.isAvailable(registrationNumber);

                    //logger.info("Car avail.: " + carAvailability);

                    if (carAvailability) {
                        session.updateCarAvailability(registrationNumber, userId);
                        UUID userIDtoCheck = session.getUserIDbyCarRegistrationNumber(registrationNumber);
//                    System.out.println(userIDtoCheck);
//                    System.out.println(userId);


                        if (userId.equals(userIDtoCheck)) {
                            logger.info("User w zglaszajacy: " + userId + " User w bazie: " + userIDtoCheck);
                        } else if (userIDtoCheck == null) {
                            logger.info("User w zglaszajacy: " + userId + " User w bazie: " + userIDtoCheck);
                        } else {
                            logger.info("User w zglaszajacy: " + userId + " User w bazie: " + userIDtoCheck);
                        }

                        //logger.info("wolne auto, zmieniam na false" + registrationNumber);
                        session.updateCarAvailabilityToDefault(registrationNumber);
                        break;
                    }


                }
                Thread.sleep(300);

            } catch (BackendException | InterruptedException e) {
                e.printStackTrace();
            }

        //stworzenie nowego auta + wynajęcie go + oddanie
        try {
            String newCarRegistrationNumber = session.createCarNew();

                boolean carAvailability = session.isAvailable(newCarRegistrationNumber);

                //logger.info("Car avail.: " + carAvailability);

                if (carAvailability) {
                    session.updateCarAvailability(newCarRegistrationNumber, userId);
                    UUID userIDtoCheck = session.getUserIDbyCarRegistrationNumber(newCarRegistrationNumber);
//                    System.out.println(userIDtoCheck);
//                    System.out.println(userId);


                    if (userId.equals(userIDtoCheck)) {
                        logger.info("User w zglaszajacy: " + userId + " User w bazie: " + userIDtoCheck);
                    } else if (userIDtoCheck == null) {
                        logger.info("User w zglaszajacy: " + userId + " User w bazie: " + userIDtoCheck);
                    } else {
                        logger.info("User w zglaszajacy: " + userId + " User w bazie: " + userIDtoCheck);
                    }

                    //logger.info("wolne auto, zmieniam na false" + registrationNumber);
                    session.updateCarAvailabilityToDefault(newCarRegistrationNumber);
                }
            Thread.sleep(300);

        } catch (BackendException | InterruptedException e) {
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
