CREATE TABLE `tquant_stock_history_quotation` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`security_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`the_date` DATE NOT NULL COMMENT '交易日',
	`amount` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易额(元)',
	`vol` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易量(股)',
	`open` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '开盘价',
	`high` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最高价',
	`low` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '最低价',
	`close` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价',
	`auto_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	`vol_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '交易量涨跌幅',
	`close_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '收盘价涨跌幅',
	`price_avg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '日均价',
	`price_avg_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '日均价涨跌幅百分比',
	`price_avg_3` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '3日均价',
	`price_avg_chg_3` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '3日均价涨跌幅百分比',
	`price_avg_5` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '5日均价',
	`price_avg_chg_5` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '5日均价涨跌幅百分比',
	`price_avg_10` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '10日均价',
	`price_avg_chg_10` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '10日均价涨跌幅百分比',
	`price_avg_chg_10_chg` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '10日均价涨跌幅百分比-涨跌幅',
	`price_avg_chg_10_chg_diff` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '10日均价涨跌幅百分比-涨跌幅-前后差',
	`money_flow` DECIMAL(20,2) NULL DEFAULT NULL COMMENT '钱流',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `security_code_the_date` (`security_code`, `the_date`),
	INDEX `auto_date` (`auto_date`),
	INDEX `close_chg` (`close_chg`),
	INDEX `price_avg_3` (`price_avg_3`),
	INDEX `price_avg_5` (`price_avg_5`),
	INDEX `price_avg_10` (`price_avg_10`)
)
COMMENT='股票历史行情'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
