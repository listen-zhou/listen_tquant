CREATE TABLE `tquant_stock_derive_kline` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`security_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`the_date` DATE NOT NULL COMMENT '交易日',
	`exchange_code` VARCHAR(20) NOT NULL COMMENT '交易所标识,SZ,SH',
	`kline_type` VARCHAR(20) NOT NULL COMMENT 'K线类型，week-周K，month-月K，quarter-季K，year-年K等',
	`amount` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易额(元)',
	`previous_amount` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '前一日交易额(元)',
	`amount_change_percent` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易额波动幅度（涨跌幅）',
	`vol` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易量(手)',
	`previous_vol` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '前一日交易量(手)',
	`vol_change_percent` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易量波动幅度（涨跌幅）',
	`open` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '开盘价',
	`high` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最高价',
	`low` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最低价',
	`close` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价',
	`previous_close` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '前一日收盘价',
	`close_change_percent` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价波动幅度（涨跌幅）',
	`auto_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `security_code_the_date_exchange_code_kline_type` (`security_code`, `the_date`, `exchange_code`, `kline_type`),
	INDEX `auto_date` (`auto_date`)
)
COMMENT='股票衍生K线数据，如周K，月K，季K，年K等'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
