CREATE DATABASE IF NOT EXISTS `access_logs_db` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE `access_logs_db`;

CREATE TABLE `access_logs` (
    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `raw_id` INT NOT NULL, -- будет хранить f_RecID
    `read_date` DATETIME NOT NULL, -- f_ReadDate
    `card_no` BIGINT DEFAULT NULL, -- f_CardNO
    `consumer_id` INT DEFAULT NULL, -- f_ConsumerID
    `character` TINYINT DEFAULT NULL, -- f_Character
    `in_out` TINYINT DEFAULT NULL, -- f_InOut (0/1)
    `status` TINYINT DEFAULT NULL, -- f_Status
    `rec_option` INT DEFAULT NULL, -- f_RecOption
    `controller_sn` BIGINT DEFAULT NULL, -- f_ControllerSN
    `reader_id` INT DEFAULT NULL, -- f_ReaderID
    `reader_no` INT DEFAULT NULL, -- f_ReaderNO
    `record_flash_loc` BIGINT DEFAULT NULL, -- f_RecordFlashLoc
    `record_all` TEXT DEFAULT NULL, -- f_RecordAll
    `modified` DATETIME DEFAULT NULL, -- f_Modified
    PRIMARY KEY (`id`),
    UNIQUE KEY `uniq_raw` (`raw_id`)
) ENGINE = INNODB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_general_ci;