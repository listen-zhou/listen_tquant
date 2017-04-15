CREATE TABLE `tquant_stock_day_kline` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`security_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`the_date` DATE NOT NULL COMMENT '交易日',
	`amount` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易额(元)',
	`amount_pre` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '前一日交易额(元)',
	`amount_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易额涨跌幅',
	`vol` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易量(手)',
	`vol_pre` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '前一日交易量(手)',
	`vol_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易量涨跌幅',
	`open` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '开盘价',
	`high` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最高价',
	`low` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最低价',
	`close` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价',
	`close_pre` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '前一日收盘价',
	`close_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价涨跌幅',
	`price_avg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '日均价',
	`close_price_avg_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘/日均价百分比',
	`price_avg_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '日均价涨跌幅百分比',
	`auto_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `security_code_the_date` (`security_code`, `the_date`),
	INDEX `auto_date` (`auto_date`)
)
COMMENT='股票日K数据'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
