/*
=================================================
SCRIPT SQL
Final Project

Nama  Team : Business Team
Anggota    : - Alwan Abdurrahman 
             - Berliana Fitria Dewi 
             - Muhammad Fakhrian Abimanyu 
             - Ryandino 
             - Satriya Fauzan Adhim

Program ini dibuat untuk membuat database dan tabel serta melakukan normalisasi di PostgreSQL. 
Adapun dataset yang dipakai adalah dataset mengenai data pasien rumah sakit. Normalisasi dibuat berdasarkan ERD yang telah dibuat.
=================================================
*/

MEMBUAT DATABASE
--------------------------------------
CREATE DATABASE final_project;


CREATE AND INSERT TABLE
---------------------------------------

-- Menghapus tabel yang mungkin ada sebelum dilakukan create tabel
DROP TABLE IF EXISTS "HospitalTrx";
DROP TABLE IF EXISTS "Doctor";
DROP TABLE IF EXISTS "Surgery";
DROP TABLE IF EXISTS "Lab";
DROP TABLE IF EXISTS "Room";
DROP TABLE IF EXISTS "Drugs";
DROP TABLE IF EXISTS "DrugType";
DROP TABLE IF EXISTS "Patient";
DROP TABLE IF EXISTS "Payment";
DROP TABLE IF EXISTS "Review";
DROP TABLE IF EXISTS "HospitalCare";
DROP TABLE IF EXISTS "Branch";


-- Membuat tabel Transaksi HospitalTrx
CREATE TABLE "HospitalTrx" (
    ID INTEGER PRIMARY KEY,  -- Primary key, tidak otomatis, perlu diisi saat insert
    Date_IN DATE,
    Date_OUT DATE,
    Branch VARCHAR(50),
    Name VARCHAR(100),
    Age VARCHAR(5),
    Gender VARCHAR(15),
    Hospital_Care VARCHAR(50),
    Room VARCHAR(20),
    Doctor VARCHAR(100),
    Surgery VARCHAR(50),
    Lab VARCHAR(50),
    Drug_Types VARCHAR(50),
    Drug_Brands VARCHAR(50),
    Drug_Qty VARCHAR(10),
    Food VARCHAR(10),
    Admin VARCHAR(10),
    COGS VARCHAR(20),
    Payment VARCHAR(20),
    Review VARCHAR(20)
);
	
-- Melakukan load data dari csv ke tabel HospitalTrx
COPY "HospitalTrx" (ID, Date_IN, Date_OUT, Branch, Name, Age, Gender, Hospital_Care, Room, Doctor, Surgery, Lab, Drug_Types, Drug_Brands, Drug_Qty, Food, Admin, COGS, Payment, Review) FROM 'C:\Users\Berliana Fitria Dewi\Desktop\Bithealth\Training Hacktiv8\Final Project\Hospital_data.csv' DELIMITER ',' CSV HEADER;

-- Membuat tabel doctor
CREATE TABLE "Doctor" (
  "doctor_id" int PRIMARY KEY,
  "doctor_type" varchar,
  "doctor_price" money
);
-- Memasukkan data doctor
INSERT INTO "Doctor" (doctor_id,doctor_type,doctor_price)
VALUES 
	(1, 'Bedah', '300000'),
	(2, 'Gigi', '300000'),
	(3, 'Kandungan', '300000'),
	(4, 'Penyakit Dalam', '300000'),
	(5, 'Umum', '200000');

-- Membuat tabel surgery
CREATE TABLE "Surgery" (
  "surgery_id" int PRIMARY KEY,
  "surgery_type" varchar,
  "surgery_price" money
);
-- Memasukkan data surgery
INSERT INTO "Surgery" (surgery_id,surgery_type,surgery_price)
VALUES 
	(1, 'Kecil', '4000000'),
	(2, 'Besar', '8000000'),
	(3, 'Kusus', '15000000');

-- Membuat tabel lab
CREATE TABLE "Lab" (
  "lab_id" int PRIMARY KEY,
  "lab_name" varchar,
  "lab_price" money
);
-- Memasukkan data lab
INSERT INTO "Lab" (lab_id,lab_name,lab_price)
VALUES 
	(1, 'Hematologi', '90000'),
	(2, 'Kimia Darah', '195000'),
	(3, 'Rontgen', '150000'),
	(4, 'Serologi', '200000'),
	(5, 'Urinalisa', '80000');

-- Membuat tabel room
CREATE TABLE "Room" (
  "room_id" int PRIMARY KEY,
  "room_type" varchar,
  "room_price" money,
  "food_price" money
);
-- Memasukkan data room
INSERT INTO "Room" (room_id,room_type,room_price,food_price)
VALUES 
	(1, 'VIP', '300000', '150000'),
	(2, 'Kelas 1', '250000', '110000'),
	(3, 'Kelas 2', '200000', '80000'),
	(4, 'Kelas 3', '150000', '50000');

-- Membuat tabel drugs
CREATE TABLE "Drugs" (
  "drug_id" int PRIMARY KEY,
  "drug_brand" varchar,
  "drug_type_id" int
);
-- Memasukkan data drugs
INSERT INTO "Drugs" (drug_id,drug_brand,drug_type_id)
VALUES 
	(1, 'Amoxicillin', 1),
	(2, 'Azithromycin', 1),
	(3, 'Blackmores', 4),
	(4, 'Calpol', 3),
	(5, 'Ciprofloxacin', 1),
	(6, 'Diclofenac', 2),
	(7, 'Enervon-C', 4),
	(8, 'Holland & Barrett', 4),
	(9, 'Naproxen', 2),
	(10,'Panadol', 3),
	(11, 'Paramex', 3),
	(12, 'Tramadol', 2);
	
-- Membuat tabel drugtype
CREATE TABLE "DrugType" (
  "drug_type_id" int PRIMARY KEY,
  "drug_type" varchar,
  "drug_price" money
);
-- Memasukkan data drugtype
INSERT INTO "DrugType" (drug_type_id,drug_type,drug_price)
VALUES 
	(1, 'Antibiotik', '75000'),
	(2, 'Pereda Nyeri', '50000'),
	(3, 'Umum', '40000'),
	(4, 'Vitamin', '110000');
	
-- Membuat tabel patient
CREATE TABLE "Patient" (
  "patient_id" SERIAL PRIMARY KEY,
  "patient_name" varchar,
  "gender" varchar,
  "age" int
);
-- Memasukkan data patient
INSERT INTO "Patient" (patient_name, gender, age)
SELECT DISTINCT name, gender, CAST(Age AS INT)
FROM "HospitalTrx";

-- Membuat tabel payment
CREATE TABLE "Payment" (
  "payment_id" int PRIMARY KEY,
  "payment_name" varchar
);
-- Memasukkan data payment
INSERT INTO "Payment" (payment_id,payment_name)
VALUES 
	(1, 'Asuransi'),
	(2, 'Pribadi');

-- Membuat tabel review
CREATE TABLE "Review" (
  "review_id" int PRIMARY KEY,
  "review_name" varchar
);
-- Memasukkan data review
INSERT INTO "Review" (review_id,review_name)
VALUES 
	(1, 'Sangat Tidak Puas'),
	(2, 'Tidak Puas'),
	(3, 'Netral'),
	(4, 'Puas'),
	(5, 'Sangat Puas');

-- Membuat tabel hospitalcare
CREATE TABLE "HospitalCare" (
  "hospitalcare_id" int PRIMARY KEY,
  "hospital_care" varchar,
  "infusion_cost" money
);
-- Memasukkan data hospitalcare
INSERT INTO "HospitalCare" (hospitalcare_id,hospital_care,infusion_cost)
VALUES 
	(1, 'Rawat Inap', '165000'),
	(2, 'Rawat Jalan', '0');

-- Membuat tabel branch
CREATE TABLE "Branch" (
  "branch_id" int PRIMARY KEY,
  "branch_name" varchar
);
-- Memasukkan data branch
INSERT INTO "Branch" (branch_id,branch_name)
VALUES
	(1, 'RSMA'),
	(2, 'RSMD'),
	(3, 'RSMS');


MELAKUKAN NORMALISASI
--------------------------------------------

ALTER TABLE "HospitalTrx" 
RENAME COLUMN id TO id_trx;
-------------------------------------------
UPDATE "HospitalTrx" AS h
SET branch = br.branch_id
FROM "Branch" AS br
WHERE h.branch = br.branch_name;

ALTER TABLE "HospitalTrx" 
RENAME COLUMN branch TO id_branch;
-----------------------------------

UPDATE "HospitalTrx" AS h
SET hospital_care = hc.hospitalcare_id
FROM "HospitalCare" AS hc
WHERE h.hospital_care = hc.hospital_care;

ALTER TABLE "HospitalTrx" 
RENAME COLUMN hospital_care TO id_hospital_care;
-----------------------------------
UPDATE "HospitalTrx" AS h
SET name = p.patient_id
FROM "Patient" AS p
WHERE h.name = p.patient_name AND h.Gender = p.gender;

ALTER TABLE "HospitalTrx" 
RENAME COLUMN name TO id_patient;
-----------------------------------

UPDATE "HospitalTrx" AS h
SET room = r.room_id
FROM "Room" AS r
WHERE h.room = r.room_type;

ALTER TABLE "HospitalTrx" 
RENAME COLUMN room TO id_room;
-----------------------------------

UPDATE "HospitalTrx" AS h
SET doctor = d.doctor_id
FROM "Doctor" AS d
WHERE h.doctor = d.doctor_type;

ALTER TABLE "HospitalTrx" 
RENAME COLUMN doctor TO id_doctor;
-----------------------------------

UPDATE "HospitalTrx" AS h
SET payment = p.payment_id
FROM "Payment" AS p
WHERE h.payment = p.payment_name;

ALTER TABLE "HospitalTrx" 
RENAME COLUMN payment TO id_payment;
-----------------------------------

UPDATE "HospitalTrx" AS h
SET surgery = s.surgery_id
FROM "Surgery" AS s
WHERE h.surgery = s.surgery_type;

ALTER TABLE "HospitalTrx" 
RENAME COLUMN surgery TO id_surgery;
-----------------------------------

UPDATE "HospitalTrx" AS h
SET lab = l.lab_id
FROM "Lab" AS l
WHERE h.lab = l.lab_name;

ALTER TABLE "HospitalTrx" 
RENAME COLUMN lab TO id_lab;
-----------------------------------
UPDATE "HospitalTrx" AS h
SET drug_brands = d.drug_id
FROM "Drugs" AS d
WHERE h.drug_brands = d.drug_brand;

ALTER TABLE "HospitalTrx" 
RENAME COLUMN drug_brands TO id_drug;
-----------------------------------

UPDATE "HospitalTrx" AS h
SET review = r.review_id
FROM "Review" AS r
WHERE h.review = r.review_name;

ALTER TABLE "HospitalTrx" 
RENAME COLUMN review TO id_review;
-------------------------
ALTER TABLE "HospitalTrx" 
DROP age,
DROP gender,
DROP drug_types,
DROP food;

ALTER TABLE "HospitalTrx" 
RENAME COLUMN admin TO admin_price;
---------------------------