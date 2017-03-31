CREATE TABLE `tquant_stock_average_line` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`security_code` VARCHAR(20) NOT NULL COMMENT '股票代码',
	`exchange_code` VARCHAR(20) NOT NULL COMMENT '交易所标识,SZ,SH',
	`the_date` DATE NOT NULL COMMENT '交易日',
	`ma` INT(11) NOT NULL COMMENT '均线类型，5-5日均线，10-10日均线等',
	`average_close` DECIMAL(20,4) NOT NULL COMMENT 'ma日均收盘价',
	`previous_average_close` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '前一日均收盘价',
	`average_close_change_percent` DECIMAL(20,4) NULL DEFAULT NULL COMMENT 'ma日均价波动幅度（涨跌幅）',
	`current_amount` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '当日成交额（元）',
	`average_amount` DECIMAL(20,4) NULL DEFAULT NULL COMMENT 'ma日均成交额(元)',
	`previous_average_amount` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '前一日均成交额(元)',
	`average_amount_change_percent` DECIMAL(20,4) NULL DEFAULT NULL COMMENT 'ma日均交易额变动幅度（涨跌幅）',
	`current_vol` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '当日成交量（元）',
	`average_vol` DECIMAL(20,4) NULL DEFAULT NULL COMMENT 'ma日均成交量(手)',
	`previous_average_vol` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '前一日均成交量(手)',
	`average_vol_change_percent` DECIMAL(20,4) NULL DEFAULT NULL COMMENT 'ma日均成交量变动幅度（涨跌幅）',
	`average_price` DECIMAL(20,4) NULL DEFAULT NULL COMMENT 'ma日均成交价价（平均成交价）',
	`previous_average_price` DECIMAL(20,4) NULL DEFAULT NULL COMMENT '前一日均成交价',
	`average_price_change_percent` DECIMAL(20,4) NULL DEFAULT NULL COMMENT 'ma日均成交价变动幅度（涨跌幅）',
	`amount_flow_percent` DECIMAL(20,4) NULL DEFAULT NULL COMMENT 'ma日金钱流向涨跌幅',
	`vol_flow_percent` DECIMAL(20,4) NULL DEFAULT NULL COMMENT 'ma日成交量流向涨跌幅',
	`auto_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `security_code_the_date_ma` (`security_code`, `the_date`, `ma`, `exchange_code`),
	INDEX `security_code` (`security_code`),
	INDEX `auto_date` (`auto_date`),
	INDEX `ma` (`ma`)
)
COMMENT='股票均线数据\r\nma日均收盘价=前ma日（含）的收盘价之和/ma\r\nma日均收盘价涨跌幅=(ma日均收盘价 - 前一日均收盘价)/前一日均收盘价 * 100\r\n\r\nma日均成交额=前ma日（含）的成交额之和/ma\r\nma日均成交额涨跌幅=(ma日均成交额 - 前一日均成交额)/前一日均成交额 * 100\r\n\r\nma日均成交量=前ma日（含）的成交量之和/ma\r\nma日均成交量涨跌幅=(ma日均成交量 - 前一日均成交量)/前一日均成交量 * 100\r\n\r\nma日均成交价=前ma日(含)的成交额之和/ma日(含)的成交额\r\nma日均成交价涨跌幅=(ma日均成交价 - 前一日均成交价)/前一日均成交价 * 100\r\n\r\nma日金钱流向涨跌幅=当日成交额/ma日(含)均成交额 * 100\r\n\r\n成交量流向涨跌幅=当日成交量/ma日(含)均成交量 * 100'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
