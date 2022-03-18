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

INSERT INTO Cars (registrationNumber, model, brand, productionYear, color) VALUES ('LLE5929','GLE Coupe 350 d 4-Matic','Mercedes-Benz','2018','white');
INSERT INTO Cars (registrationNumber, model, brand, productionYear, color) VALUES ('WX91806','Seria 5 530d','BMW','2005','grey');
INSERT INTO Cars (registrationNumber, model, brand, productionYear, color) VALUES ('NOL9272','Capture 1.0 TCe Zen','Renualt','2020','copper');
INSERT INTO Cars (registrationNumber, model, brand, productionYear, color) VALUES ('NOL9273','Capture 1.0 TCe Zen','Renualt','2020','copper');
INSERT INTO Cars (registrationNumber, model, brand, productionYear, color) VALUES ('NOL9274','Capture 1.0 TCe Zen','Renualt','2020','copper');
INSERT INTO Cars (registrationNumber, model, brand, productionYear, color) VALUES ('NOL9275','Capture 1.0 TCe Zen','Renualt','2020','copper');
INSERT INTO Cars (registrationNumber, model, brand, productionYear, color) VALUES ('NOL9276','Capture 1.0 TCe Zen','Renualt','2020','copper');
INSERT INTO Cars (registrationNumber, model, brand, productionYear, color) VALUES ('NOL9277','Capture 1.0 TCe Zen','Renualt','2020','copper');
INSERT INTO Cars (registrationNumber, model, brand, productionYear, color) VALUES ('CIN1938','A110','Alpine','2020','black');
INSERT INTO Cars (registrationNumber, model, brand, productionYear, color) VALUES ('PKL9977','Diablo','Lamborghini','1993','red');
INSERT INTO Cars (registrationNumber, model, brand, productionYear, color) VALUES ('SCZ0047','Mustang 5.0 V8 GT','Ford','2016','red');

CREATE TABLE Reservation_Cars (
  rs_id uuid,
  registrationNumber varchar,
  model varchar, 
  brand varchar,
  user_id uuid,
  PRIMARY KEY(user_id)
);

CREATE TABLE car_status (
  registrationNumber varchar,
  status boolean,
  user_id uuid,
  PRIMARY KEY(registrationNumber)
);