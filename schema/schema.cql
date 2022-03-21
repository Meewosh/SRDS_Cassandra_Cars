CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

USE Test;

CREATE TABLE Cars (
  registrationNumber varchar,
  model varchar,
  brand varchar,
  productionYear varchar,
  color varchar,
  PRIMARY KEY((brand, model), registrationNumber)
);

CREATE TABLE Cars_by_registrationNumber (
  registrationNumber varchar,
  model varchar,
  brand varchar,
  productionYear varchar,
  color varchar,
  PRIMARY KEY(registrationNumber)
);

CREATE TABLE Reservation_Cars (
  rs_id uuid,
  registrationNumber varchar,
  user_id uuid,
  PRIMARY KEY(user_id, rs_id)
);

CREATE TABLE car_status (
  registrationNumber varchar,
  status boolean,
  user_id uuid,
  PRIMARY KEY(registrationNumber)
);