CREATE TABLE `tquant_process_progress_info` (
	`id` INT(11) NOT NULL AUTO_INCREMENT COMMENT '主键自增',
	`flag` VARCHAR(50) NULL DEFAULT NULL COMMENT '跑批标记',
	`security_code` VARCHAR(20) NULL DEFAULT NULL COMMENT '证券代码',
	`auto_date` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	PRIMARY KEY (`id`),
	INDEX `auto_date` (`auto_date`),
	INDEX `flag` (`flag`)
)
COMMENT='跑批处理进度信息'
ENGINE=InnoDB
;
