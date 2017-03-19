CREATE TABLE `tquant_stock_average_line` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`security_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`exchange_code` VARCHAR(20) NOT NULL COMMENT '交易所标识,SZ,SH',
	`the_date` DATE NOT NULL COMMENT '交易日',
	`ma` INT(11) NOT NULL COMMENT '均线类型，5-5日均线，10-10日均线等',
	`price` DECIMAL(20,4) NOT NULL COMMENT '均值',
	`auto_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `security_code_the_date_ma` (`security_code`, `the_date`, `ma`, `exchange_code`),
	INDEX `security_code` (`security_code`),
	INDEX `auto_date` (`auto_date`),
	INDEX `ma` (`ma`)
)
COMMENT='股票均线数据'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
