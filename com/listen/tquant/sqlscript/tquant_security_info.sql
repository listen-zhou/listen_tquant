CREATE TABLE `tquant_security_info` (
	`id` INT(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`security_code` VARCHAR(20) NOT NULL COMMENT '证券代码',
	`security_name` VARCHAR(200) NULL DEFAULT NULL COMMENT '证券简称',
	`exchange_code` VARCHAR(20) NULL DEFAULT NULL COMMENT '交易所标识,SZ,SH',
	`worth_buying` INT(11) NULL DEFAULT NULL COMMENT '是否值得买,1-值得，其他-不值得',
	`auto_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `security_code` (`security_code`),
	INDEX `auto_date` (`auto_date`),
	INDEX `security_name` (`security_name`)
)
COMMENT='证券基本信息表'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
ALTER TABLE `tquant_security_info`
	ADD COLUMN `sumilated_stock_condition` VARCHAR(2000) NULL DEFAULT NULL COMMENT '模拟炒股条件json字符串' AFTER `worth_buying`;
