ALTER TABLE `tquant_security_info`
	ADD COLUMN `worth_buying` INT NOT NULL COMMENT '是否值得买,1-值得，其他-不值得' AFTER `exchange_code`;