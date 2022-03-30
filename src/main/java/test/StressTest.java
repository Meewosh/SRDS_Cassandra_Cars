package test;
import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
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


        ResultSet queryAll = null;
        ResultSet carReservationForConcreteUser = null;

        // Stress test ma nacelu pokazac, ze przy odczycie z bazy danych zaraz po zapisie istenieje szansa odczytamy pusta wartosc
        // przy Consistency level ONE

        for (int j = 0; j < 100; j++) {
            int randomNumberOfCarBrand = rand1.nextInt(9);
            int randomNumberOfCarModel = rand2.nextInt(9);
            try {
                queryAll = session.
                        selectConcreteCarAndCheckAvailability(carTableBrand[randomNumberOfCarBrand], carTableModel[randomNumberOfCarModel]);
                for (Row row : queryAll) {
                    String registrationNumber = row.getString("registrationNumber");

                    UUID reservationId = UUID.randomUUID();
                    //update bazy danych Car Reservation
                    if(session.updateCarReservation(registrationNumber, userId, reservationId, "2022-03-27")) {

                        UUID userIdToCheck = session.getUserIDbyCarRegistrationNumber(registrationNumber);


                        if (userId.equals(userIdToCheck)) {
                            session.insertIntoReservationByUser(reservationId, registrationNumber,
                                    userId, "2022-03-27", "2022-05-15", carTableBrand[randomNumberOfCarBrand], carTableModel[randomNumberOfCarModel]);

                            String registrationNumberCheck = session.selectConcreteReservationByUserIdAndReservationId(userId, reservationId);
                            if (registrationNumber.equals(registrationNumberCheck)) {
                                logger.info("Dane wprowadzone do bazy Reservation Cars oraz Reservation Cars by User");
                            } else {
                                logger.warn("Dane wprowadzone do bazy Reservation Cars, ale nie wprowadzone do  Reservation Cars by User");
                            }
                        } else if (userIdToCheck == null) {
                            logger.warn("Odczytanie wartosci null w bazie danych");
                        } else if (userIdToCheck != userId) {
                            logger.warn("Auto zostalo zarezerwowane dla innego uzytkowanika. User: " + userId + " User w bazie: ");
                        }
                        break;
                    }

                }
                Thread.sleep(300);

            } catch (BackendException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("Koniec");

    }
}
