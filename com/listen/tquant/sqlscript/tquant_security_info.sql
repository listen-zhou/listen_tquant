CREATE TABLE `tquant_security_info` (
	`id` INT(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`security_code` VARCHAR(20) NOT NULL COMMENT '证券代码',
	`security_name` VARCHAR(200) NOT NULL COMMENT '证券简称',
	`security_type` VARCHAR(20) NOT NULL COMMENT '证券类型,STOCK,INDEX,FUND',
	`exchange_code` VARCHAR(20) NOT NULL COMMENT '交易所标识,SZ,SH',
	`auto_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `security_code_security_type_exchange_code` (`security_code`, `security_type`, `exchange_code`),
	INDEX `auto_date` (`auto_date`),
	INDEX `security_name` (`security_name`)
)
COMMENT='证券基本信息表'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
