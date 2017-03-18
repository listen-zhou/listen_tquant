CREATE TABLE `tquant_calendar_info` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
	`the_date` DATE NOT NULL COMMENT '交易日（年月日）',
	`is_month_end` INT(11) NOT NULL COMMENT '是否是月度末的最后一天，1-是，0-否',
	`is_month_start` INT(11) NOT NULL COMMENT '是否是月度始的第一天，1-是，0-否',
	`is_quarter_end` INT(11) NOT NULL COMMENT '是否是季度末的最后一天，1-是，0-否',
	`is_quarter_start` INT(11) NOT NULL COMMENT '是否是季度始的第一天，1-是，0-否',
	`is_year_end` INT(11) NOT NULL COMMENT '是否是年末的最后一天，1-是，0-否',
	`is_year_start` INT(11) NOT NULL COMMENT '是否年始的第一天，1-是，0-否',
	`day_of_week` INT(11) NOT NULL COMMENT '第几天（一周中的），周一是0',
	`week_of_year` INT(11) NOT NULL COMMENT '第几周（一年中的）',
	`quarter` INT(11) NOT NULL COMMENT '季度（数字）',
	`auto_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `the_date` (`the_date`),
	INDEX `auto_date` (`auto_date`)
)
COMMENT='证券交易日表\r\n'
COLLATE='utf8_general_ci'
ENGINE=InnoDB
;
