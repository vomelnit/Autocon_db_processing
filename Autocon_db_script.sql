
CREATE DATABASE Au_db CHARACTER SET utf8 COLLATE utf8_general_ci;


create table aa_transactions (
	id INT UNSIGNED primary key auto_increment,
	user_id INT UNSIGNED not null,
	brand_to_type_id INT UNSIGNED not null,
	tr_year SMALLINT UNSIGNED not null ,
	tr_month TINYINT UNSIGNED not null ,
	tr_day TINYINT UNSIGNED not null ,
	region_id INT not null,
	amount_of_units INT not null
);



INSERT INTO aa_transactions (user_id,brand_to_type_id,tr_year,tr_month,tr_day,region_id,amount_of_units)
 VALUES ('1','1','2020','1','1','1','150'),('1','2','2020','1','1','1','77'),('1','3','2020','1','1','1','44'),('1','4','2020','1','1','1','55'),('1','5','2020','1','1','1','66'),('1','1','2020','1','2','1','134'),('1','2','2020','1','2','1','66'),('1','1','2019','12','1','1','130'),('1','2','2019','12','2','1','99'),('1','3','2019','12','1','1','210');


create table aa_vehicle_type(
 	id INT UNSIGNED primary key auto_increment,
	title varchar(100) not null);
INSERT INTO aa_vehicle_type (title) VALUES('Cars'),('LCV');


create table aa_brands(
 	id INT UNSIGNED primary key auto_increment,
	name varchar(100) not null);

INSERT INTO aa_brands (name) VALUES('Toyota'),('Renault'),('Skoda'),('Mazda');

create table aa_brand_to_vt (
 	id INT UNSIGNED primary key auto_increment,
	brand_id INT UNSIGNED not null,
	vt_id INT UNSIGNED not null);

INSERT INTO aa_brand_to_vt (brand_id,vt_id) VALUES(1,1),(2,1),(3,2),(4,1),(3,1);

create table aa_diagram (
	id INT UNSIGNED primary key auto_increment,
	dg_year SMALLINT UNSIGNED not null ,
	dg_month TINYINT UNSIGNED not null ,
	dg_day TINYINT UNSIGNED not null ,
	vehicle_type_id varchar(100) not null,
	amount_of_units INT UNSIGNED null,
	dynamic_compared_to_previous_month float not null,
	dynamic_compared_to_previous_year float not null
);

create table Volume_of_market (
	id INT UNSIGNED primary key auto_increment,
	year SMALLINT UNSIGNED not null ,
	month TINYINT UNSIGNED not null ,
	type_id INT UNSIGNED not null,
	amount_of_units INT UNSIGNED null
);

INSERT INTO Volume_of_market (year,month,type_id,amount_of_units) VALUES (2019,1,2,5295),(2019,2,2,5780),(2019,3,2,7355),(2019,4,2,6911),(2019,5,2,6710),(2019,6,2,6845),(2019,7,2,7730),(2019,8,2,8322),(2019,9,2,7092),(2019,10,2,9142),(2019,11,2,8870),(2019,12,2,9410),(2019,1,1,614),(2019,2,1,511),(2019,3,1,673),(2019,4,1,601),(2019,5,1,676),(2019,6,1,593),(2019,7,1,721),(2019,8,1,665),(2019,9,1,656),(2019,10,1,743),(2019,11,1,730),(2019,12,1,961);

/****************************************/
CREATE DATABASE Au_db CHARACTER SET utf8 COLLATE utf8_general_ci;

create table Types (
	type_id INT UNSIGNED primary key,
	type_name varchar(100),
	UNIQUE (type_name)
);
INSERT INTO Types (type_id,type_name) VALUES (0,'Легковые авто'),(1,'Коммерческие авто');


create table Volume_of_market (
	volume_of_market_id INT UNSIGNED primary key auto_increment,
	year SMALLINT UNSIGNED not null ,
	month TINYINT UNSIGNED not null ,
	type_id INT UNSIGNED not null,
	amount_of_units INT UNSIGNED null,
	constraint fk_volume_of_market_types foreign key (type_id) references Types(type_id),
	UNIQUE (year,month,type_id)
);

INSERT INTO Volume_of_market (year,month,type_id,amount_of_units) VALUES (2019,1,0,5295),(2019,2,0,5780),(2019,3,0,7355),(2019,4,0,6911),(2019,5,0,6710),(2019,6,0,6845),(2019,7,0,7730),(2019,8,0,8322),(2019,9,0,7092),(2019,10,0,9142),(2019,11,0,8870),(2019,12,0,9410),(2019,1,1,614),(2019,2,1,511),(2019,3,1,673),(2019,4,1,601),(2019,5,1,676),(2019,6,1,593),(2019,7,1,721),(2019,8,1,665),(2019,9,1,656),(2019,10,1,743),(2019,11,1,730),(2019,12,1,961);

create table Brands (
	brand_id INT UNSIGNED primary key auto_increment,
	name varchar(100) not null,
	type_id INT UNSIGNED not null,
	UNIQUE (name,type_id)
	
);

INSERT INTO Brands (name,type_id)
 VALUES ('Toyota',0),('Renault',0),('Peugeot',0),('Citroen',0),('DS',0),('FAW',0),('Volkswagen',0),('Audi',0),('SEAT',0),('Skoda',0),('Toyota',1),('Renault',1),('Volkswagen',1);

create table Users (
	user_id INT UNSIGNED primary key auto_increment,
	name varchar(100),
	brand_id INT UNSIGNED,
	constraint fk_users_brands foreign key (brand_id) references Brands(brand_id)
);

INSERT INTO Users (name,brand_id)
 VALUES ('Таращук Роман Янович',1),('Гаврилов Олесь Михайлович',2),('Гедіанов Борис Михайлович',3),('Мустафін Вадим Антонович',4),('Туабе Ігор Андрійович',5),('Карголомський Олесь Михайлович',6),('Омельченко Борис Михайлович',6),('Сазонов Микита Олексійович',7),('Ширинкий Роман Янович',8),('Губін Олесь Михайлович',9),('Жедринський Борис Михайлович',10),('Таращук Роман Янович',11),('Гаврилов Олесь Михайлович',12),('Гедіанов Борис Михайлович',13);

create table Transactions (
	transaction_id INT UNSIGNED primary key auto_increment,
	user_id INT UNSIGNED not null,
	brand_id INT UNSIGNED not null,
	year SMALLINT UNSIGNED not null ,
	month TINYINT UNSIGNED not null ,
	period varchar(30) not null ,
	region varchar(100) not null,
	type_name varchar(100) not null,
	amount_of_units INT not null,
	time_of_transaction DATETIME not null,
	constraint fk_transactions_users foreign key (user_id) references Users(user_id),
	constraint fk_transactions_brands foreign key (brand_id) references Brands(brand_id),
	UNIQUE (year,month,period,brand_id,type_name)
);

INSERT INTO Transactions (user_id,brand_id,year,month,period,region,type_name,amount_of_units,time_of_transaction)
 VALUES ('1','1','2020','1','7 дней','Украина','Легковые авто','150', NOW()),
('2','2','2020','1','7 дней','Украина','Легковые авто','170', NOW()),
('3','3','2020','1','7 дней','Украина','Легковые авто','20', NOW()),
('4','4','2020','1','7 дней','Украина','Легковые авто','25', NOW()),
('6','6','2020','1','7 дней','Украина','Легковые авто','10', NOW()),
('8','8','2020','1','7 дней','Украина','Легковые авто','30', NOW()),
('10','10','2020','1','7 дней','Украина','Легковые авто','140', NOW()),
('1','1','2020','1','7 дней','Украина','Коммерческие авто','150', NOW()),('2','2','2020','1','7 дней','Украина','Коммерческие авто','170', NOW()),
('3','3','2020','1','7 дней','Украина','Коммерческие авто','20', NOW());

INSERT INTO Transactions (user_id,brand_id,year,month,period,region,type_name,amount_of_units,time_of_transaction)
 VALUES ('1','1','2020','1','21 дней','Украина','Легковые авто',350, NOW()),
('2','2','2020','1','14 дней','Украина','Легковые авто','300', NOW()),
('3','3','2020','1','14 дней','Украина','Легковые авто','45', NOW()),
('4','4','2020','1','14 дней','Украина','Легковые авто','55', NOW()),
('6','6','2020','1','14 дней','Украина','Легковые авто','15', NOW()),
('8','8','2020','1','14 дней','Украина','Легковые авто','40', NOW()),
('10','10','2020','1','14 дней','Украина','Легковые авто','210', NOW()),
('11','11','2020','1','14 дней','Украина','Коммерческие авто','6', NOW()),
('12','12','2020','1','14 дней','Украина','Коммерческие авто','45', NOW()),
('13','13','2020','1','14 дней','Украина','Коммерческие авто','18', NOW());

INSERT INTO Transactions (user_id,brand_id,year,month,period,region,type_name,amount_of_units,time_of_transaction)
 VALUES ('1','1','2019','1','21 дней','Украина','Легковые авто',334, NOW()),
('2','2','2019','12','14 дней','Украина','Легковые авто','323', NOW()),
('3','3','2019','12','14 дней','Украина','Легковые авто','55', NOW()),
('4','4','2019','12','14 дней','Украина','Легковые авто','45', NOW()),
('6','6','2019','12','14 дней','Украина','Легковые авто','20', NOW()),
('8','8','2019','12','14 дней','Украина','Легковые авто','34', NOW()),
('10','10','2019','12','14 дней','Украина','Легковые авто','299', NOW()),
('11','11','2019','12','14 дней','Украина','Коммерческие авто','2', NOW()),
('12','12','2019','12','14 дней','Украина','Коммерческие авто','40', NOW()),
('13','13','2019','12','14 дней','Украина','Коммерческие авто','24', NOW());


create table Diagram (
	diagram_id INT UNSIGNED primary key auto_increment,
	year SMALLINT UNSIGNED not null ,
	month TINYINT UNSIGNED not null ,
	period varchar(30) not null ,
	type_name varchar(100) not null,
	amount_of_brands SMALLINT UNSIGNED not null,
	amount_of_units INT UNSIGNED null,
	pridicted_market_volume INT UNSIGNED not null,
	dynamic_compared_to_previous_month float not null,
	dynamic_compared_to_previous_year float not null,
	UNIQUE (year,month,period,type_name)
);



create table Rating (
	rating_id INT UNSIGNED primary key auto_increment,
	year SMALLINT UNSIGNED not null,
	month TINYINT UNSIGNED not null,
	type_id INT UNSIGNED not null,
	type_name varchar(100) not null,
	brand_id INT UNSIGNED not null,
	brand_name varchar(100) not null,
	amount_for_7_days INT UNSIGNED null,
	amount_for_14_days INT UNSIGNED null,
	amount_for_21_days INT UNSIGNED null,
	amount_for_28_days INT UNSIGNED null,
	amount_for_month INT UNSIGNED null,
	constraint fk_rating_users foreign key (type_id) references Types(type_id),
	constraint fk_rating_brands foreign key (brand_id) references Brands(brand_id),
	UNIQUE (year,month,brand_id,type_id)
);

create table Python_activation_time (
	python_activation_time_id INT UNSIGNED primary key auto_increment,
	time_of_activation DATETIME not null
	
);

