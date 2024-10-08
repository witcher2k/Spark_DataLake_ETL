
/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

CREATE DATABASE /*!32312 IF NOT EXISTS*/`custpayments` /*!40100 DEFAULT CHARACTER SET latin1 */;

USE `custpayments`;

/*Table structure for table `customers` */

DROP TABLE IF EXISTS `customers`;

CREATE TABLE `customers` (
  `customerNumber` int(11) NOT NULL,
  `customerName` varchar(50) NOT NULL,
  `contactLastName` varchar(50) NOT NULL,
  `contactFirstName` varchar(50) NOT NULL,
  `phone` varchar(50) NOT NULL,
  `addressLine1` varchar(50) NOT NULL,
  `addressLine2` varchar(50) DEFAULT NULL,
  `city` varchar(50) NOT NULL,
  `state` varchar(50) DEFAULT NULL,
  `postalCode` varchar(15) DEFAULT NULL,
  `country` varchar(50) NOT NULL,
  `salesRepEmployeeNumber` int(11) DEFAULT NULL,
  `creditLimit` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`customerNumber`),
  KEY `salesRepEmployeeNumber` (`salesRepEmployeeNumber`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*Data for the table `customers` */

insert  into `customers`(`customerNumber`,`customerName`,`contactLastName`,`contactFirstName`,`phone`,`addressLine1`,`addressLine2`,`city`,`state`,`postalCode`,`country`,`salesRepEmployeeNumber`,`creditLimit`) values 

(103,'Atelier graphique','Schmitt','Carine ','40.32.2555','54, rue Royale',NULL,'Nantes',NULL,'44000','France',1370,'21000.00'),

(112,'Signal Gift Stores','King','Jean','7025551838','8489 Strong St.',NULL,'Las Vegas','NV','83030','USA',1166,'71800.00'),

(114,'Australian Collectors, Co.','Ferguson','Peter','03 9520 4555','636 St Kilda Road','Level 3','Melbourne','Victoria','3004','Australia',1611,'117300.00'),

(119,'La Rochelle Gifts','Labrune','Janine ','40.67.8555','67, rue des Cinquante Otages',NULL,'Nantes',NULL,'44000','France',1370,'118200.00'),

(121,'Baane Mini Imports','Bergulfsen','Jonas ','07-98 9555','Erling Skakkes gate 78',NULL,'Stavern',NULL,'4110','Norway',1504,'81700.00'),

(124,'Mini Gifts Distributors Ltd.','Nelson','Susan','4155551450','5677 Strong St.',NULL,'San Rafael','CA','97562','USA',1165,'210500.00'),

(125,'Havel & Zbyszek Co','Piestrzeniewicz','Zbyszek ','(26) 642-7555','ul. Filtrowa 68',NULL,'Warszawa',NULL,'01-012','Poland',NULL,'0.00'),

(128,'Blauer See Auto, Co.','Keitel','Roland','+49 69 66 90 2555','Lyonerstr. 34',NULL,'Frankfurt',NULL,'60528','Germany',1504,'59700.00'),

(129,'Mini Wheels Co.','Murphy','Julie','6505555787','5557 North Pendale Street',NULL,'San Francisco','CA','94217','USA',1165,'64600.00'),

(131,'Land of Toys Inc.','Lee','Kwai','2125557818','897 Long Airport Avenue',NULL,'NYC','NY','10022','USA',1323,'114900.00'),

(141,'Euro+ Shopping Channel','Freyre','Diego ','(91) 555 94 44','C/ Moralzarzal, 86',NULL,'Madrid',NULL,'28034','Spain',1370,'227600.00'),

(144,'Volvo Model Replicas, Co','Berglund','Christina ','0921-12 3555','Berguvsvägen  8',NULL,'Luleå',NULL,'S-958 22','Sweden',1504,'53100.00'),

(145,'Danish Wholesale Imports','Petersen','Jytte ','31 12 3555','Vinbæltet 34',NULL,'Kobenhavn',NULL,'1734','Denmark',1401,'83400.00'),

(146,'Saveley & Henriot, Co.','Saveley','Mary ','78.32.5555','2, rue du Commerce',NULL,'Lyon',NULL,'69004','France',1337,'123900.00'),

(148,'Dragon Souveniers, Ltd.','Natividad','Eric','+65 221 7555','Bronz Sok.','Bronz Apt. 3/6 Tesvikiye','Singapore',NULL,'079903','Singapore',1621,'103800.00'),

(151,'Muscle Machine Inc','Young','Jeff','2125557413','4092 Furth Circle','Suite 400','NYC','NY','10022','USA',1286,'138500.00'),

(157,'Diecast Classics Inc.','Leong','Kelvin','2155551555','7586 Pompton St.',NULL,'Allentown','PA','70267','USA',1216,'100600.00'),

(161,'Technics Stores Inc.','Hashimoto','Juri','6505556809','9408 Furth Circle',NULL,'Burlingame','CA','94217','USA',1165,'84600.00'),

(166,'Handji Gifts& Co','Victorino','Wendy','+65 224 1555','106 Linden Road Sandown','2nd Floor','Singapore',NULL,'069045','Singapore',1612,'97900.00'),

(167,'Herkku Gifts','Oeztan','Veysel','+47 2267 3215','Brehmen St. 121','PR 334 Sentrum','Bergen',NULL,'N 5804','Norway  ',1504,'96800.00'),

(168,'American Souvenirs Inc','Franco','Keith','2035557845','149 Spinnaker Dr.','Suite 101','New Haven','CT','97823','USA',1286,'0.00'),

(169,'Porto Imports Co.','de Castro','Isabel ','(1) 356-5555','Estrada da saúde n. 58',NULL,'Lisboa',NULL,'1756','Portugal',NULL,'0.00'),

(171,'Daedalus Designs Imports','Rancé','Martine ','20.16.1555','184, chaussée de Tournai',NULL,'Lille',NULL,'59000','France',1370,'82900.00'),

(172,'La Corne D\'abondance, Co.','Bertrand','Marie','(1) 42.34.2555','265, boulevard Charonne',NULL,'Paris',NULL,'75012','France',1337,'84300.00'),

(173,'Cambridge Collectables Co.','Tseng','Jerry','6175555555','4658 Baden Av.',NULL,'Cambridge','MA','51247','USA',1188,'43400.00'),

(175,'Gift Depot Inc.','King','Julie','2035552570','25593 South Bay Ln.',NULL,'Bridgewater','CT','97562','USA',1323,'84300.00'),

(177,'Osaka Souveniers Co.','Kentary','Mory','+81 06 6342 5555','1-6-20 Dojima',NULL,'Kita-ku','Osaka',' 530-0003','Japan',1621,'81200.00'),

(181,'Vitachrome Inc.','Frick','Michael','2125551500','2678 Kingston Rd.','Suite 101','NYC','NY','10022','USA',1286,'76400.00'),

(186,'Toys of Finland, Co.','Karttunen','Matti','90-224 8555','Keskuskatu 45',NULL,'Helsinki',NULL,'21240','Finland',1501,'96500.00'),

(187,'AV Stores, Co.','Ashworth','Rachel','(171) 555-1555','Fauntleroy Circus',NULL,'Manchester',NULL,'EC2 5NT','UK',1501,'136800.00'),

(189,'Clover Collections, Co.','Cassidy','Dean','+353 1862 1555','25 Maiden Lane','Floor No. 4','Dublin',NULL,'2','Ireland',1504,'69400.00'),

(198,'Auto-Moto Classics Inc.','Taylor','Leslie','6175558428','16780 Pompton St.',NULL,'Brickhaven','MA','58339','USA',1216,'23000.00'),

(201,'UK Collectables, Ltd.','Devon','Elizabeth','(171) 555-2282','12, Berkeley Gardens Blvd',NULL,'Liverpool',NULL,'WX1 6LT','UK',1501,'92700.00'),

(202,'Canadian Gift Exchange Network','Tamuri','Yoshi ','(604) 555-3392','1900 Oak St.',NULL,'Vancouver','BC','V3F 2K1','Canada',1323,'90300.00'),

(204,'Online Mini Collectables','Barajas','Miguel','6175557555','7635 Spinnaker Dr.',NULL,'Brickhaven','MA','58339','USA',1188,'68700.00'),

(205,'Toys4GrownUps.com','Young','Julie','6265557265','78934 Hillside Dr.',NULL,'Pasadena','CA','90003','USA',1166,'90700.00'),

(206,'Asian Shopping Network, Co','Walker','Brydey','+612 9411 1555','Suntec Tower Three','8 Temasek','Singapore',NULL,'038988','Singapore',NULL,'0.00'),

(209,'Mini Caravy','Citeaux','Frédérique ','88.60.1555','24, place Kléber',NULL,'Strasbourg',NULL,'67000','France',1370,'53800.00'),

(211,'King Kong Collectables, Co.','Gao','Mike','+852 2251 1555','Bank of China Tower','1 Garden Road','Central Hong Kong',NULL,NULL,'Hong Kong',1621,'58600.00'),

(216,'Enaco Distributors','Saavedra','Eduardo ','(93) 203 4555','Rambla de Cataluña, 23',NULL,'Barcelona',NULL,'08022','Spain',1702,'60300.00'),

(219,'Boards & Toys Co.','Young','Mary','3105552373','4097 Douglas Av.',NULL,'Glendale','CA','92561','USA',1166,'11000.00'),

(223,'Natürlich Autos','Kloss','Horst ','0372-555188','Taucherstraße 10',NULL,'Cunewalde',NULL,'01307','Germany',NULL,'0.00'),

(227,'Heintze Collectables','Ibsen','Palle','86 21 3555','Smagsloget 45',NULL,'Århus',NULL,'8200','Denmark',1401,'120800.00'),

(233,'Québec Home Shopping Network','Fresnière','Jean ','(514) 555-8054','43 rue St. Laurent',NULL,'Montréal','Québec','H1J 1C3','Canada',1286,'48700.00'),

(237,'ANG Resellers','Camino','Alejandra ','(91) 745 6555','Gran Vía, 1',NULL,'Madrid',NULL,'28001','Spain',NULL,'0.00'),

(239,'Collectable Mini Designs Co.','Thompson','Valarie','7605558146','361 Furth Circle',NULL,'San Diego','CA','91217','USA',1166,'105000.00'),

(240,'giftsbymail.co.uk','Bennett','Helen ','(198) 555-8888','Garden House','Crowther Way 23','Cowes','Isle of Wight','PO31 7PJ','UK',1501,'93900.00'),

(242,'Alpha Cognac','Roulet','Annette ','61.77.6555','1 rue Alsace-Lorraine',NULL,'Toulouse',NULL,'31000','France',1370,'61100.00'),

(247,'Messner Shopping Network','Messner','Renate ','069-0555984','Magazinweg 7',NULL,'Frankfurt',NULL,'60528','Germany',NULL,'0.00'),

(249,'Amica Models & Co.','Accorti','Paolo ','011-4988555','Via Monte Bianco 34',NULL,'Torino',NULL,'10100','Italy',1401,'113000.00'),

(250,'Lyon Souveniers','Da Silva','Daniel','+33 1 46 62 7555','27 rue du Colonel Pierre Avia',NULL,'Paris',NULL,'75508','France',1337,'68100.00'),

(256,'Auto Associés & Cie.','Tonini','Daniel ','30.59.8555','67, avenue de l\'Europe',NULL,'Versailles',NULL,'78000','France',1370,'77900.00'),

(259,'Toms Spezialitäten, Ltd','Pfalzheim','Henriette ','0221-5554327','Mehrheimerstr. 369',NULL,'Köln',NULL,'50739','Germany',1504,'120400.00'),

(260,'Royal Canadian Collectables, Ltd.','Lincoln','Elizabeth ','(604) 555-4555','23 Tsawassen Blvd.',NULL,'Tsawassen','BC','T2F 8M4','Canada',1323,'89600.00'),

(273,'Franken Gifts, Co','Franken','Peter ','089-0877555','Berliner Platz 43',NULL,'München',NULL,'80805','Germany',NULL,'0.00'),

(276,'Anna\'s Decorations, Ltd','O\'Hara','Anna','02 9936 8555','201 Miller Street','Level 15','North Sydney','NSW','2060','Australia',1611,'107800.00'),

(278,'Rovelli Gifts','Rovelli','Giovanni ','035-640555','Via Ludovico il Moro 22',NULL,'Bergamo',NULL,'24100','Italy',1401,'119600.00'),

(282,'Souveniers And Things Co.','Huxley','Adrian','+61 2 9495 8555','Monitor Money Building','815 Pacific Hwy','Chatswood','NSW','2067','Australia',1611,'93300.00'),

(286,'Marta\'s Replicas Co.','Hernandez','Marta','6175558555','39323 Spinnaker Dr.',NULL,'Cambridge','MA','51247','USA',1216,'123700.00'),

(293,'BG&E Collectables','Harrison','Ed','+41 26 425 50 01','Rte des Arsenaux 41 ',NULL,'Fribourg',NULL,'1700','Switzerland',NULL,'0.00'),

(298,'Vida Sport, Ltd','Holz','Mihael','0897-034555','Grenzacherweg 237',NULL,'Genève',NULL,'1203','Switzerland',1702,'141300.00'),

(299,'Norway Gifts By Mail, Co.','Klaeboe','Jan','+47 2212 1555','Drammensveien 126A','PB 211 Sentrum','Oslo',NULL,'N 0106','Norway  ',1504,'95100.00'),

(303,'Schuyler Imports','Schuyler','Bradley','+31 20 491 9555','Kingsfordweg 151',NULL,'Amsterdam',NULL,'1043 GR','Netherlands',NULL,'0.00'),

(307,'Der Hund Imports','Andersen','Mel','030-0074555','Obere Str. 57',NULL,'Berlin',NULL,'12209','Germany',NULL,'0.00'),

(311,'Oulu Toy Supplies, Inc.','Koskitalo','Pirkko','981-443655','Torikatu 38',NULL,'Oulu',NULL,'90110','Finland',1501,'90500.00'),

(314,'Petit Auto','Dewey','Catherine ','(02) 5554 67','Rue Joseph-Bens 532',NULL,'Bruxelles',NULL,'B-1180','Belgium',1401,'79900.00'),

(319,'Mini Classics','Frick','Steve','9145554562','3758 North Pendale Street',NULL,'White Plains','NY','24067','USA',1323,'102700.00'),

(320,'Mini Creations Ltd.','Huang','Wing','5085559555','4575 Hillside Dr.',NULL,'New Bedford','MA','50553','USA',1188,'94500.00'),

(321,'Corporate Gift Ideas Co.','Brown','Julie','6505551386','7734 Strong St.',NULL,'San Francisco','CA','94217','USA',1165,'105000.00'),

(323,'Down Under Souveniers, Inc','Graham','Mike','+64 9 312 5555','162-164 Grafton Road','Level 2','Auckland  ',NULL,NULL,'New Zealand',1612,'88000.00'),

(324,'Stylish Desk Decors, Co.','Brown','Ann ','(171) 555-0297','35 King George',NULL,'London',NULL,'WX3 6FW','UK',1501,'77000.00'),

(328,'Tekni Collectables Inc.','Brown','William','2016559350','7476 Moss Rd.',NULL,'Newark','NJ','94019','USA',1323,'43000.00'),

(333,'Australian Gift Network, Co','Calaghan','Ben','61-7-3844-6555','31 Duncan St. West End',NULL,'South Brisbane','Queensland','4101','Australia',1611,'51600.00'),

(334,'Suominen Souveniers','Suominen','Kalle','+358 9 8045 555','Software Engineering Center','SEC Oy','Espoo',NULL,'FIN-02271','Finland',1501,'98800.00'),

(335,'Cramer Spezialitäten, Ltd','Cramer','Philip ','0555-09555','Maubelstr. 90',NULL,'Brandenburg',NULL,'14776','Germany',NULL,'0.00'),

(339,'Classic Gift Ideas, Inc','Cervantes','Francisca','2155554695','782 First Street',NULL,'Philadelphia','PA','71270','USA',1188,'81100.00'),

(344,'CAF Imports','Fernandez','Jesus','+34 913 728 555','Merchants House','27-30 Merchant\'s Quay','Madrid',NULL,'28023','Spain',1702,'59600.00'),

(347,'Men \'R\' US Retailers, Ltd.','Chandler','Brian','2155554369','6047 Douglas Av.',NULL,'Los Angeles','CA','91003','USA',1166,'57700.00'),

(348,'Asian Treasures, Inc.','McKenna','Patricia ','2967 555','8 Johnstown Road',NULL,'Cork','Co. Cork',NULL,'Ireland',NULL,'0.00'),

(350,'Marseille Mini Autos','Lebihan','Laurence ','91.24.4555','12, rue des Bouchers',NULL,'Marseille',NULL,'13008','France',1337,'65000.00'),

(353,'Reims Collectables','Henriot','Paul ','26.47.1555','59 rue de l\'Abbaye',NULL,'Reims',NULL,'51100','France',1337,'81100.00'),

(356,'SAR Distributors, Co','Kuger','Armand','+27 21 550 3555','1250 Pretorius Street',NULL,'Hatfield','Pretoria','0028','South Africa',NULL,'0.00'),

(357,'GiftsForHim.com','MacKinlay','Wales','64-9-3763555','199 Great North Road',NULL,'Auckland',NULL,NULL,'New Zealand',1612,'77700.00'),

(361,'Kommission Auto','Josephs','Karin','0251-555259','Luisenstr. 48',NULL,'Münster',NULL,'44087','Germany',NULL,'0.00'),

(362,'Gifts4AllAges.com','Yoshido','Juri','6175559555','8616 Spinnaker Dr.',NULL,'Boston','MA','51003','USA',1216,'41900.00'),

(363,'Online Diecast Creations Co.','Young','Dorothy','6035558647','2304 Long Airport Avenue',NULL,'Nashua','NH','62016','USA',1216,'114200.00'),

(369,'Lisboa Souveniers, Inc','Rodriguez','Lino ','(1) 354-2555','Jardim das rosas n. 32',NULL,'Lisboa',NULL,'1675','Portugal',NULL,'0.00'),

(376,'Precious Collectables','Urs','Braun','0452-076555','Hauptstr. 29',NULL,'Bern',NULL,'3012','Switzerland',1702,'0.00'),

(379,'Collectables For Less Inc.','Nelson','Allen','6175558555','7825 Douglas Av.',NULL,'Brickhaven','MA','58339','USA',1188,'70700.00'),

(381,'Royale Belge','Cartrain','Pascale ','(071) 23 67 2555','Boulevard Tirou, 255',NULL,'Charleroi',NULL,'B-6000','Belgium',1401,'23500.00'),

(382,'Salzburg Collectables','Pipps','Georg ','6562-9555','Geislweg 14',NULL,'Salzburg',NULL,'5020','Austria',1401,'71700.00'),

(385,'Cruz & Sons Co.','Cruz','Arnold','+63 2 555 3587','15 McCallum Street','NatWest Center #13-03','Makati City',NULL,'1227 MM','Philippines',1621,'81500.00'),

(386,'L\'ordine Souveniers','Moroni','Maurizio ','0522-556555','Strada Provinciale 124',NULL,'Reggio Emilia',NULL,'42100','Italy',1401,'121400.00'),

(398,'Tokyo Collectables, Ltd','Shimamura','Akiko','+81 3 3584 0555','2-2-8 Roppongi',NULL,'Minato-ku','Tokyo','106-0032','Japan',1621,'94400.00'),

(406,'Auto Canal+ Petit','Perrier','Dominique','(1) 47.55.6555','25, rue Lauriston',NULL,'Paris',NULL,'75016','France',1337,'95000.00'),

(409,'Stuttgart Collectable Exchange','Müller','Rita ','0711-555361','Adenauerallee 900',NULL,'Stuttgart',NULL,'70563','Germany',NULL,'0.00'),

(412,'Extreme Desk Decorations, Ltd','McRoy','Sarah','04 499 9555','101 Lambton Quay','Level 11','Wellington',NULL,NULL,'New Zealand',1612,'86800.00'),

(415,'Bavarian Collectables Imports, Co.','Donnermeyer','Michael',' +49 89 61 08 9555','Hansastr. 15',NULL,'Munich',NULL,'80686','Germany',1504,'77000.00'),

(424,'Classic Legends Inc.','Hernandez','Maria','2125558493','5905 Pompton St.','Suite 750','NYC','NY','10022','USA',1286,'67500.00'),

(443,'Feuer Online Stores, Inc','Feuer','Alexander ','0342-555176','Heerstr. 22',NULL,'Leipzig',NULL,'04179','Germany',NULL,'0.00'),

(447,'Gift Ideas Corp.','Lewis','Dan','2035554407','2440 Pompton St.',NULL,'Glendale','CT','97561','USA',1323,'49700.00'),

(448,'Scandinavian Gift Ideas','Larsson','Martha','0695-34 6555','Åkergatan 24',NULL,'Bräcke',NULL,'S-844 67','Sweden',1504,'116400.00'),

(450,'The Sharp Gifts Warehouse','Frick','Sue','4085553659','3086 Ingle Ln.',NULL,'San Jose','CA','94217','USA',1165,'77600.00'),

(452,'Mini Auto Werke','Mendel','Roland ','7675-3555','Kirchgasse 6',NULL,'Graz',NULL,'8010','Austria',1401,'45300.00'),

(455,'Super Scale Inc.','Murphy','Leslie','2035559545','567 North Pendale Street',NULL,'New Haven','CT','97823','USA',1286,'95400.00'),

(456,'Microscale Inc.','Choi','Yu','2125551957','5290 North Pendale Street','Suite 200','NYC','NY','10022','USA',1286,'39800.00'),

(458,'Corrida Auto Replicas, Ltd','Sommer','Martín ','(91) 555 22 82','C/ Araquil, 67',NULL,'Madrid',NULL,'28023','Spain',1702,'104600.00'),

(459,'Warburg Exchange','Ottlieb','Sven ','0241-039123','Walserweg 21',NULL,'Aachen',NULL,'52066','Germany',NULL,'0.00'),

(462,'FunGiftIdeas.com','Benitez','Violeta','5085552555','1785 First Street',NULL,'New Bedford','MA','50553','USA',1216,'85800.00'),

(465,'Anton Designs, Ltd.','Anton','Carmen','+34 913 728555','c/ Gobelas, 19-1 Urb. La Florida',NULL,'Madrid',NULL,'28023','Spain',NULL,'0.00'),

(471,'Australian Collectables, Ltd','Clenahan','Sean','61-9-3844-6555','7 Allen Street',NULL,'Glen Waverly','Victoria','3150','Australia',1611,'60300.00'),

(473,'Frau da Collezione','Ricotti','Franco','+39 022515555','20093 Cologno Monzese','Alessandro Volta 16','Milan',NULL,NULL,'Italy',1401,'34800.00'),

(475,'West Coast Collectables Co.','Thompson','Steve','3105553722','3675 Furth Circle',NULL,'Burbank','CA','94019','USA',1166,'55400.00'),

(477,'Mit Vergnügen & Co.','Moos','Hanna ','0621-08555','Forsterstr. 57',NULL,'Mannheim',NULL,'68306','Germany',NULL,'0.00'),

(480,'Kremlin Collectables, Co.','Semenov','Alexander ','+7 812 293 0521','2 Pobedy Square',NULL,'Saint Petersburg',NULL,'196143','Russia',NULL,'0.00'),

(481,'Raanan Stores, Inc','Altagar,G M','Raanan','+ 972 9 959 8555','3 Hagalim Blv.',NULL,'Herzlia',NULL,'47625','Israel',NULL,'0.00'),

(484,'Iberia Gift Imports, Corp.','Roel','José Pedro ','(95) 555 82 82','C/ Romero, 33',NULL,'Sevilla',NULL,'41101','Spain',1702,'65700.00'),

(486,'Motor Mint Distributors Inc.','Salazar','Rosa','2155559857','11328 Douglas Av.',NULL,'Philadelphia','PA','71270','USA',1323,'72600.00'),

(487,'Signal Collectibles Ltd.','Taylor','Sue','4155554312','2793 Furth Circle',NULL,'Brisbane','CA','94217','USA',1165,'60300.00'),

(489,'Double Decker Gift Stores, Ltd','Smith','Thomas ','(171) 555-7555','120 Hanover Sq.',NULL,'London',NULL,'WA1 1DP','UK',1501,'43300.00'),

(495,'Diecast Collectables','Franco','Valarie','6175552555','6251 Ingle Ln.',NULL,'Boston','MA','51003','USA',1188,'85100.00'),

(496,'Kelly\'s Gift Shop','Snowden','Tony','+64 9 5555500','Arenales 1938 3\'A\'',NULL,'Auckland  ',NULL,NULL,'New Zealand',1612,'110000.00');

/*Table structure for table `payments` */

DROP TABLE IF EXISTS `payments`;

CREATE TABLE `payments` (
  `customerNumber` int(11) NOT NULL,
  `checkNumber` varchar(50) NOT NULL,
  `paymentDate` date NOT NULL,
  `amount` decimal(10,2) NOT NULL,
  PRIMARY KEY (`customerNumber`,`checkNumber`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*Data for the table `payments` */

insert  into `payments`(`customerNumber`,`checkNumber`,`paymentDate`,`amount`) values 

(103,'HQ336336','2022-10-19','6066.78'),

(103,'JM555205','2022-10-05','14571.44'),

(103,'OM314933','2022-10-18','1676.14'),

(112,'BO864823','2022-10-17','14191.12'),

(112,'HQ55022','2022-10-06','32641.98'),

(112,'ND748579','2022-10-20','33347.88'),

(114,'GG31455','2022-10-20','45864.03'),

(114,'MA765515','2022-10-15','82261.22'),

(114,'NP603840','2022-10-31','7565.08'),

(114,'NR27552','2022-10-10','44894.74'),

(119,'DB933704','2022-10-14','19501.82'),

(119,'LN373447','2022-10-08','47924.19'),

(119,'NG94694','2022-10-22','49523.67'),

(121,'DB889831','2022-10-16','50218.95'),

(121,'FD317790','2022-10-28','1491.38'),

(121,'KI831359','2022-10-04','17876.32'),

(121,'MA302151','2022-10-28','34638.14'),

(124,'AE215433','2022-10-05','101244.59'),

(124,'BG255406','2022-10-28','85410.87'),

(124,'CQ287967','2022-10-11','11044.30'),

(124,'ET64396','2022-10-16','83598.04'),

(124,'HI366474','2022-10-27','47142.70'),

(124,'HR86578','2022-10-02','55639.66'),

(124,'KI131716','2022-10-15','111654.40'),

(124,'LF217299','2022-10-26','43369.30'),

(124,'NT141748','2022-10-25','45084.38'),

(128,'DI925118','2022-10-28','10549.01'),

(128,'FA465482','2022-10-18','24101.81'),

(128,'FH668230','2022-10-24','33820.62'),

(128,'IP383901','2022-10-18','7466.32'),

(129,'DM826140','2022-10-08','26248.78'),

(129,'ID449593','2022-10-11','23923.93'),

(129,'PI42991','2022-10-09','16537.85'),

(131,'CL442705','2022-10-12','22292.62'),

(131,'MA724562','2022-10-02','50025.35'),

(131,'NB445135','2022-10-11','35321.97'),

(141,'AU364101','2022-10-19','36251.03'),

(141,'DB583216','2022-10-01','36140.38'),

(141,'DL460618','2022-10-19','46895.48'),

(141,'HJ32686','2022-10-30','59830.55'),

(141,'ID10962','2022-10-31','116208.40'),

(141,'IN446258','2022-10-25','65071.26'),

(141,'JE105477','2022-10-18','120166.58'),

(141,'JN355280','2022-10-26','49539.37'),

(141,'JN722010','2022-10-25','40206.20'),

(141,'KT52578','2022-10-09','63843.55'),

(141,'MC46946','2022-10-09','35420.74'),

(141,'MF629602','2022-10-16','20009.53'),

(141,'NU627706','2022-10-17','26155.91'),

(144,'IR846303','2022-10-12','36005.71'),

(144,'LA685678','2022-10-09','7674.94'),

(145,'CN328545','2022-10-03','4710.73'),

(145,'ED39322','2022-10-26','28211.70'),

(145,'HR182688','2022-10-01','20564.86'),

(145,'JJ246391','2022-10-20','53959.21'),

(146,'FP549817','2022-10-18','40978.53'),

(146,'FU793410','2022-10-16','49614.72'),

(146,'LJ160635','2022-10-10','39712.10'),

(148,'BI507030','2022-10-22','44380.15'),

(148,'DD635282','2022-10-11','2611.84'),

(148,'KM172879','2022-10-26','105743.00'),

(148,'ME497970','2022-10-27','3516.04'),

(151,'BF686658','2022-10-22','58793.53'),

(151,'GB852215','2022-10-26','20314.44'),

(151,'IP568906','2022-10-18','58841.35'),

(151,'KI884577','2022-10-14','39964.63'),

(157,'HI618861','2022-10-19','35152.12'),

(157,'NN711988','2022-10-07','63357.13'),

(161,'BR352384','2022-10-14','2434.25'),

(161,'BR478494','2022-10-18','50743.65'),

(161,'KG644125','2022-10-02','12692.19'),

(161,'NI908214','2022-10-05','38675.13'),

(166,'BQ327613','2022-10-16','38785.48'),

(166,'DC979307','2022-10-07','44160.92'),

(166,'LA318629','2022-10-28','22474.17'),

(167,'ED743615','2022-10-19','12538.01'),

(167,'GN228846','2022-10-03','85024.46'),

(171,'GB878038','2022-10-15','18997.89'),

(171,'IL104425','2022-10-22','42783.81'),

(172,'AD832091','2022-10-09','1960.80'),

(172,'CE51751','2022-10-04','51209.58'),

(172,'EH208589','2022-10-20','33383.14'),

(173,'GP545698','2022-10-13','11843.45'),

(173,'IG462397','2022-10-29','20355.24'),

(175,'CITI3434344','2022-10-19','28500.78'),

(175,'IO448913','2022-10-19','24879.08'),

(175,'PI15215','2022-10-10','42044.77'),

(177,'AU750837','2022-10-17','15183.63'),

(177,'CI381435','2022-10-19','47177.59'),

(181,'CM564612','2022-10-25','22602.36'),

(181,'GQ132144','2022-10-30','5494.78'),

(181,'OH367219','2022-10-16','44400.50'),

(186,'AE192287','2022-10-10','23602.90'),

(186,'AK412714','2022-10-27','37602.48'),

(186,'KA602407','2022-10-21','34341.08'),

(187,'AM968797','2022-10-03','52825.29'),

(187,'BQ39062','2022-10-08','47159.11'),

(187,'KL124726','2022-10-27','48425.69'),

(189,'BO711618','2022-10-03','17359.53'),

(189,'NM916675','2022-10-01','32538.74'),

(198,'FI192930','2022-10-06','9658.74'),

(198,'HQ920225','2022-10-06','6036.96'),

(198,'IS946883','2022-10-21','5858.56'),

(201,'DP677013','2022-10-20','23908.24'),

(201,'OO846801','2022-10-15','37258.94'),

(202,'HI358554','2022-10-18','36527.61'),

(202,'IQ627690','2022-10-08','33594.58'),

(204,'GC697638','2022-10-13','51152.86'),

(204,'IS150005','2022-10-24','4424.40'),

(205,'GL756480','2022-10-04','3879.96'),

(205,'LL562733','2022-10-05','50342.74'),

(205,'NM739638','2022-10-06','39580.60'),

(209,'BOAF82044','2022-10-03','35157.75'),

(209,'ED520529','2022-10-21','4632.31'),

(209,'PH785937','2022-10-04','36069.26'),

(211,'BJ535230','2022-10-09','45480.79'),

(216,'BG407567','2022-10-09','3101.40'),

(216,'ML780814','2022-10-06','24945.21'),

(216,'MM342086','2022-10-14','40473.86'),

(219,'BN17870','2022-10-02','3452.75'),

(219,'BR941480','2022-10-18','4465.85'),

(227,'MQ413968','2022-10-31','36164.46'),

(227,'NU21326','2022-10-02','53745.34'),

(233,'BOFA23232','2022-10-20','29070.38'),

(233,'II180006','2022-10-01','22997.45'),

(233,'JG981190','2022-10-18','16909.84'),

(239,'NQ865547','2022-10-15','80375.24'),

(240,'IF245157','2022-10-16','46788.14'),

(240,'JO719695','2022-10-28','24995.61'),

(242,'AF40894','2022-10-22','33818.34'),

(242,'HR224331','2022-10-03','12432.32'),

(242,'KI744716','2022-10-21','14232.70'),

(249,'IJ399820','2022-10-19','33924.24'),

(249,'NE404084','2022-10-04','48298.99'),

(250,'EQ12267','2022-10-17','17928.09'),

(250,'HD284647','2022-10-30','26311.63'),

(250,'HN114306','2022-10-18','23419.47'),

(256,'EP227123','2022-10-10','5759.42'),

(256,'HE84936','2022-10-22','53116.99'),

(259,'EU280955','2022-10-06','61234.67'),

(259,'GB361972','2022-10-07','27988.47'),

(260,'IO164641','2022-10-30','37527.58'),

(260,'NH776924','2022-10-24','29284.42'),

(276,'EM979878','2022-10-09','27083.78'),

(276,'KM841847','2022-10-13','38547.19'),

(276,'LE432182','2022-10-28','41554.73'),

(276,'OJ819725','2022-10-30','29848.52'),

(278,'BJ483870','2022-10-05','37654.09'),

(278,'GP636783','2022-10-02','52151.81'),

(278,'NI983021','2022-10-24','37723.79'),

(282,'IA793562','2022-10-03','24013.52'),

(282,'JT819493','2022-10-02','35806.73'),

(282,'OD327378','2022-10-03','31835.36'),

(286,'DR578578','2022-10-28','47411.33'),

(286,'KH910279','2022-10-05','43134.04'),

(298,'AJ574927','2022-10-13','47375.92'),

(298,'LF501133','2022-10-18','61402.00'),

(299,'AD304085','2022-10-24','36798.88'),

(299,'NR157385','2022-10-05','32260.16'),

(311,'DG336041','2022-10-15','46770.52'),

(311,'FA728475','2022-10-06','32723.04'),

(311,'NQ966143','2022-10-25','16212.59'),

(314,'LQ244073','2022-10-09','45352.47'),

(314,'MD809704','2022-10-03','16901.38'),

(319,'HL685576','2022-10-06','42339.76'),

(319,'OM548174','2022-10-07','36092.40'),

(320,'GJ597719','2022-10-18','8307.28'),

(320,'HO576374','2022-10-20','41016.75'),

(320,'MU817160','2022-10-24','52548.49'),

(321,'DJ15149','2022-10-03','85559.12'),

(321,'LA556321','2022-10-15','46781.66'),

(323,'AL493079','2022-10-23','75020.13'),

(323,'ES347491','2022-10-24','37281.36'),

(323,'HG738664','2022-10-05','2880.00'),

(323,'PQ803830','2022-10-24','39440.59'),

(324,'DQ409197','2022-10-13','13671.82'),

(324,'FP443161','2022-10-07','29429.14'),

(324,'HB150714','2022-10-23','37455.77'),

(328,'EN930356','2022-10-16','7178.66'),

(328,'NR631421','2022-10-30','31102.85'),

(333,'HL209210','2022-10-15','23936.53'),

(333,'JK479662','2022-10-17','9821.32'),

(333,'NF959653','2022-10-01','21432.31'),

(334,'CS435306','2022-10-27','45785.34'),

(334,'HH517378','2022-10-16','29716.86'),

(334,'LF737277','2022-10-22','28394.54'),

(339,'AP286625','2022-10-24','23333.06'),

(339,'DA98827','2022-10-28','34606.28'),

(344,'AF246722','2022-10-24','31428.21'),

(344,'NJ906924','2022-10-02','15322.93'),

(347,'DG700707','2022-10-18','21053.69'),

(347,'LG808674','2022-10-24','20452.50'),

(350,'BQ602907','2022-10-11','18888.31'),

(350,'CI471510','2022-10-25','50824.66'),

(350,'OB648482','2022-10-29','1834.56'),

(353,'CO351193','2022-10-10','49705.52'),

(353,'ED878227','2022-10-21','13920.26'),

(353,'GT878649','2022-10-21','16700.47'),

(353,'HJ618252','2022-10-09','46656.94'),

(357,'AG240323','2022-10-16','20220.04'),

(357,'NB291497','2022-10-15','36442.34'),

(362,'FP170292','2022-10-11','18473.71'),

(362,'OG208861','2022-10-21','15059.76'),

(363,'HL575273','2022-10-17','50799.69'),

(363,'IS232033','2022-10-16','10223.83'),

(363,'PN238558','2022-10-05','55425.77'),

(379,'CA762595','2022-10-12','28322.83'),

(379,'FR499138','2022-10-16','32680.31'),

(379,'GB890854','2022-10-02','12530.51'),

(381,'BC726082','2022-10-03','12081.52'),

(381,'CC475233','2022-10-19','1627.56'),

(381,'GB117430','2022-10-03','14379.90'),

(381,'MS154481','2022-10-22','1128.20'),

(382,'CC871084','2022-10-12','35826.33'),

(382,'CT821147','2022-10-01','6419.84'),

(382,'PH29054','2022-10-27','42813.83'),

(385,'BN347084','2022-10-02','20644.24'),

(385,'CP804873','2022-10-19','15822.84'),

(385,'EK785462','2022-10-09','51001.22'),

(386,'DO106109','2022-10-18','38524.29'),

(386,'HG438769','2022-10-18','51619.02'),

(398,'AJ478695','2022-10-14','33967.73'),

(398,'DO787644','2022-10-21','22037.91'),

(398,'JPMR4544','2022-10-18','615.45'),

(398,'KB54275','2022-10-29','48927.64'),

(406,'BJMPR4545','2022-10-23','12190.85'),

(406,'HJ217687','2022-10-28','49165.16'),

(406,'NA197101','2022-10-17','25080.96'),

(412,'GH197075','2022-10-25','35034.57'),

(412,'PJ434867','2022-10-14','31670.37'),

(415,'ER54537','2022-10-28','31310.09'),

(424,'KF480160','2022-10-07','25505.98'),

(424,'LM271923','2022-10-16','21665.98'),

(424,'OA595449','2022-10-31','22042.37'),

(447,'AO757239','2022-10-15','6631.36'),

(447,'ER615123','2022-10-25','17032.29'),

(447,'OU516561','2022-10-17','26304.13'),

(448,'FS299615','2022-10-18','27966.54'),

(448,'KR822727','2022-10-30','48809.90'),

(450,'EF485824','2022-10-21','59551.38'),

(452,'ED473873','2022-10-15','27121.90'),

(452,'FN640986','2022-10-20','15130.97'),

(452,'HG635467','2022-10-03','8807.12'),

(455,'HA777606','2022-10-05','38139.18'),

(455,'IR662429','2022-10-12','32239.47'),

(456,'GJ715659','2022-10-13','27550.51'),

(456,'MO743231','2022-10-30','1679.92'),

(458,'DD995006','2022-10-15','33145.56'),

(458,'NA377824','2022-10-06','22162.61'),

(458,'OO606861','2022-10-13','57131.92'),

(462,'ED203908','2022-10-15','30293.77'),

(462,'GC60330','2022-10-08','9977.85'),

(462,'PE176846','2022-10-27','48355.87'),

(471,'AB661578','2022-10-28','9415.13'),

(471,'CO645196','2022-10-10','35505.63'),

(473,'LL427009','2022-10-17','7612.06'),

(473,'PC688499','2022-10-27','17746.26'),

(475,'JP113227','2022-10-09','7678.25'),

(475,'PB951268','2022-10-13','36070.47'),

(484,'GK294076','2022-10-26','3474.66'),

(484,'JH546765','2022-10-29','47513.19'),

(486,'BL66528','2022-10-14','5899.38'),

(486,'HS86661','2022-10-23','45994.07'),

(486,'JB117768','2022-10-20','25833.14'),

(487,'AH612904','2022-10-28','29997.09'),

(487,'PT550181','2022-10-29','12573.28'),

(489,'OC773849','2022-10-04','22275.73'),

(489,'PO860906','2022-10-31','7310.42'),

(495,'BH167026','2022-10-26','59265.14'),

(495,'FN155234','2022-10-14','6276.60'),

(496,'EU531600','2022-10-25','30253.75'),

(496,'MB342426','2022-10-16','32077.44'),

(496,'MN89921','2022-10-31','52166.00');
