CREATE TABLE `tquant_stock_day_kline` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`security_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`the_date` DATE NOT NULL COMMENT '交易日',
	`exchange_code` VARCHAR(20) NOT NULL COMMENT '交易所标识,SZ,SH',
	`amount` DECIMAL(20,4) NOT NULL COMMENT '交易额(万元)',
	`vol` DECIMAL(20,4) NOT NULL COMMENT '交易量(万手)',
	`open` DECIMAL(10,4) NOT NULL COMMENT '开盘价',
	`high` DECIMAL(10,4) NOT NULL COMMENT '最高价',
	`low` DECIMAL(10,4) NOT NULL COMMENT '最低价',
	`close` DECIMAL(10,4) NOT NULL COMMENT '收盘价',
	`auto_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `security_code_the_date` (`security_code`, `the_date`, `exchange_code`),
	INDEX `security_code` (`security_code`),
	INDEX `auto_date` (`auto_date`)
)
COMMENT='股票日K数据'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
