CREATE TABLE `tquant_stock_average_line` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`security_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`exchange_code` VARCHAR(20) NOT NULL COMMENT '交易所标识,SZ,SH',
	`the_date` DATE NOT NULL COMMENT '交易日',
	`ma` INT(11) NOT NULL COMMENT '均线类型，5-5日均线，10-10日均线等',
	`close` DECIMAL(20,4) NOT NULL COMMENT '均价',
	`previous_close` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '前一日收盘价',
	`close_change_percent` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '均价波动幅度（涨跌幅）',
	`amount` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '均交易额(万元)',
	`previous_amount` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '前一日交易额(万元)',
	`amount_change_percent` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '均交易额变动幅度（涨跌幅）',
	`vol` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '均交易量(万手)',
	`previous_vol` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '前一日交易量(万手)',
	`vol_change_percent` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '均交易量变动幅度（涨跌幅）',
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
