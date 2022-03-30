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
  day timestamp,
  PRIMARY KEY(registrationNumber, day)
);

CREATE TABLE Reservation_by_user (
  rs_id uuid,
  registrationNumber varchar,
  user_id uuid,
  dateFrom varchar,
  dateTo varchar,
  brand varchar,
  model varchar,
  PRIMARY KEY(user_id, rs_id)
);
