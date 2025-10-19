--
-- Table structure for table `role_kpis`
--

DROP TABLE IF EXISTS `role_kpis`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `role_kpis` (
  `id` int NOT NULL AUTO_INCREMENT,
  `role` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `kpi_name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `weightage` int NOT NULL,
  `kpi_type` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `role_kpi_name` (`role`,`kpi_name`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `role_kpis`
--

LOCK TABLES `role_kpis` WRITE;
/*!40000 ALTER TABLE `role_kpis` DISABLE KEYS */;
INSERT INTO `role_kpis` VALUES (1,'attender','Cleanliness',50,'manual'),(2,'attender','Attitude, Behavior & Discipline',30,'manual'),(3,'attender','Insurance Target',20,'target_based');
/*!40000 ALTER TABLE `role_kpis` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `kpi_evaluations`
--

DROP TABLE IF EXISTS `kpi_evaluations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `kpi_evaluations` (
  `id` int NOT NULL AUTO_INCREMENT,
  `period` varchar(7) COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `role_kpi_id` int NOT NULL,
  `score` decimal(5,2) NOT NULL,
  `evaluator_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  KEY `role_kpi_id` (`role_kpi_id`),
  KEY `evaluator_id` (`evaluator_id`),
  CONSTRAINT `kpi_evaluations_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`),
  CONSTRAINT `kpi_evaluations_ibfk_2` FOREIGN KEY (`role_kpi_id`) REFERENCES `role_kpis` (`id`),
  CONSTRAINT `kpi_evaluations_ibfk_3` FOREIGN KEY (`evaluator_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
