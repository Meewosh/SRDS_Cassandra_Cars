package test;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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

        List<String> listOfCarBrands = createListOfCarBrands();
        List<String> listOfCarModels = createListOfCarModels();

        ResultSet queryAllCarsWithPickedBrandAndModel = null;


        // Stress test ma nacelu pokazac, ze przy odczycie z bazy danych zaraz po zapisie istenieje szansa odczytamy pusta wartosc
        // przy Consistency level ONE

        for (int j = 0; j < 3000; j++) {

            String randomCarBrand = randomizeCar(listOfCarBrands);
            String randomCarModel = randomizeCar(listOfCarModels);

            try {
                queryAllCarsWithPickedBrandAndModel = session.selectConcreteCar(randomCarBrand, randomCarModel);

                Row randomRow = getRandomRowFromResultSet(queryAllCarsWithPickedBrandAndModel);

                String registrationNumber = randomRow.getString("registrationNumber");

                String randomDate = generateRandomDate();

                boolean isCarAvailable = checkCarAvailability(registrationNumber, randomDate);

                if (isCarAvailable) {
                    UUID reservationId = UUID.randomUUID();
                    createNewReservationForCar(reservationId, userId, registrationNumber, randomDate);
                    boolean isCarCorrectlyReserved = checkCarReservation(reservationId, userId, registrationNumber, randomDate);

                    if (isCarCorrectlyReserved) {
                        logger.info("Car sucessfully added to reservation_cars");
                        createNewReservationForUser(reservationId, userId, registrationNumber, randomDate, randomCarBrand, randomCarModel);
                    }
                } else {
                    logger.info("Car is not available");
                }

            } catch (BackendException e) {
                e.printStackTrace();
            }
        }
        logger.info("Koniec");

    }

    private void createNewReservationForUser(UUID reservationId, UUID userId, String registrationNumber, String randomDate, String randomCarBrand, String randomCarModel) throws BackendException {
        session.insertIntoReservationByUser(reservationId, registrationNumber, userId, randomDate, randomCarBrand, randomCarModel);
    }

    private boolean checkCarReservation(UUID reservationId, UUID userId, String registrationNumber, String randomDate) throws BackendException {
        return session.getSpecificCarReservation(registrationNumber, randomDate, reservationId, userId);
    }


    private void createNewReservationForCar(UUID reservationId, UUID userId, String registrationNumber, String randomDate) throws BackendException {
        session.insertIntoCarsReservation(reservationId, userId, registrationNumber, randomDate);
    }

    private boolean checkCarAvailability(String registrationNumber, String randomDate) throws BackendException {
        return !session.isCarReservedForDate(registrationNumber, randomDate);
    }

    private String generateRandomDate() {
        Random random = new Random();
        int minDay = (int) LocalDate.of(2022, 4, 1).toEpochDay();
        int maxDay = (int) LocalDate.of(2022, 4, 30).toEpochDay();
        long randomDay = minDay + random.nextInt(maxDay - minDay);
        LocalDate randomDate = LocalDate.ofEpochDay(randomDay);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return randomDate.format(formatter);
    }

    private Row getRandomRowFromResultSet(ResultSet queryAllCarsWithPickedBrandAndModel) {
        List<Row> allRowsList = queryAllCarsWithPickedBrandAndModel.all();
        int randomIndex = new Random().nextInt(allRowsList.size());
        return allRowsList.get(randomIndex);
    }

    private String randomizeCar(List<String> listOfCars) {
        int randomIndex = new Random().nextInt(listOfCars.size());
        return listOfCars.get(randomIndex);
    }

    private List<String> createListOfCarBrands() {

        List<String> listOfCarBrands = new ArrayList<>();

        listOfCarBrands.add("Mercedes-Benz");
        listOfCarBrands.add("BMW");
        listOfCarBrands.add("Renualt");
        listOfCarBrands.add("Lamborghini");
        listOfCarBrands.add("Ford");
        listOfCarBrands.add("Volkswagen");
        listOfCarBrands.add("Audi");
        listOfCarBrands.add("Hyundai");
        listOfCarBrands.add("Kia");

        return listOfCarBrands;
    }

    private List<String> createListOfCarModels() {

        List<String> listOfCarModels = new ArrayList<>();

        listOfCarModels.add("1050e small");
        listOfCarModels.add("1586w big");
        listOfCarModels.add("6947aa medium");
        listOfCarModels.add("123");
        listOfCarModels.add("1");
        listOfCarModels.add("332w");
        listOfCarModels.add("Speed");
        listOfCarModels.add("Off-road");
        listOfCarModels.add("Electric");

        return listOfCarModels;
    }
}
