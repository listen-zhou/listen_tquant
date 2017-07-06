CREATE TABLE `tquant_stock_history_quotation` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`security_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`the_date` DATE NOT NULL COMMENT '交易日',
	`close` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价',
	`close_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价幅',
	`close_open_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价与开盘价幅',
	`open` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '开盘价',
	`open_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '开盘价幅',
	`high` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最高价',
	`high_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最高价幅',
	`low` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最低价',
	`low_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最低价幅',
	`amount` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易额(元)',
	`amount_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易额幅',
	`vol` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易量(手)',
	`vol_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易量幅',
	`week_day` INT(11) NULL DEFAULT NULL COMMENT '周几',
	`price_avg_1` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '日均价',
	`price_avg_1_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '日均价涨跌幅',
	`price_avg_1_chg_diff` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '日均价涨跌幅差',
	`close_price_avg_1_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘与日均价幅',
	`price_avg_3` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '3日均价',
	`price_avg_3_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '3日均价涨跌幅',
	`price_avg_3_chg_diff` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '3日均价涨跌幅差',
	`close_price_avg_3_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘与3日均价幅',
	`price_avg_5` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '5日均价',
	`price_avg_5_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '5日均价涨跌幅',
	`price_avg_5_chg_diff` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '5日均价涨跌幅差',
	`close_price_avg_5_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘与5日均价幅',
	`price_avg_10` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '10日均价',
	`price_avg_10_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '10日均价涨跌幅',
	`price_avg_10_chg_diff` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '10日均价涨跌幅差',
	`close_price_avg_10_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘与10日均价幅',
	`auto_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `security_code_the_date` (`security_code`, `the_date`),
	INDEX `auto_date` (`auto_date`)
)
COMMENT='股票历史行情'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
AUTO_INCREMENT=78580
;
