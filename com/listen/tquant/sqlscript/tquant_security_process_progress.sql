CREATE TABLE `tquant_security_process_progress` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`business_type` VARCHAR(100) NOT NULL COMMENT '业务类型,如STOCK_DAY_KLINE 股票日K数据处理',
	`security_code` VARCHAR(20) NOT NULL COMMENT '证券代码',
	`security_type` VARCHAR(20) NOT NULL COMMENT '证券类型,STOCK,INDEX,FUND',
	`exchange_code` VARCHAR(20) NOT NULL COMMENT '交易所标识,SZ,SH',
	`process_progress` INT(11) NOT NULL COMMENT '处理进度标示,1-完毕，其他-未完',
	`auto_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `business_type_security_code_security_type` (`business_type`, `security_code`, `security_type`, `exchange_code`),
	INDEX `security_code` (`security_code`),
	INDEX `security_type` (`security_type`),
	INDEX `auto_date` (`auto_date`),
	INDEX `business_type` (`business_type`)
)
COMMENT='股票业务处理进度记录表\r\n如：根据股票基本信息表查询处理股票的日K数据，由于量比较大，可能多次处理，就需要记录已经处理完毕的股票代码\r\n下次再进行处理相同业务的时候，查询上次处理的点，继续处理即可'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
