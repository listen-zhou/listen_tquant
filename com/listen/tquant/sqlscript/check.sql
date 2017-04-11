select kline.the_date, kline.open, kline.high, kline.low, kline.close, kline.close_chg, kline.close_price_avg_chg, ma10close_ma_price_avg_chg, 
kline.amount, kline.vol, 
kline.price_avg, kline.price_avg_chg, 
ma3_price_avg, ma3_price_avg_chg, 

ma5_price_avg, ma5_price_avg_chg,


ma10_price_avg, ma10_price_avg_chg, ma10_price_avg_chg_avg 


from 
tquant_stock_day_kline kline 
left join 
(
select security_code, the_date, price_avg ma3_price_avg, price_avg_chg ma3_price_avg_chg
 from tquant_stock_average_line 
where ma = 3 and security_code = '002466' and the_date >= '2017-03-09' and the_date <= '2017-04-11'
) ma3 
on kline.security_code = ma3.security_code and kline.the_date = ma3.the_date
left join 
(select security_code, the_date, price_avg ma5_price_avg, price_avg_chg ma5_price_avg_chg from tquant_stock_average_line 
where ma = 5 and security_code = '002466' and the_date >= '2017-03-09' and the_date <= '2017-04-11'
) ma5 
on kline.security_code = ma5.security_code and kline.the_date = ma5.the_date
left join 
(select security_code, the_date, price_avg ma10_price_avg, price_avg_chg ma10_price_avg_chg, 
price_avg_chg_avg ma10_price_avg_chg_avg, close_ma_price_avg_chg ma10close_ma_price_avg_chg 
 from tquant_stock_average_line 
where ma = 10 and security_code = '002466' and the_date >= '2017-03-09' and the_date <= '2017-04-11'
) ma10 
on kline.security_code = ma10.security_code and kline.the_date = ma10.the_date
order by kline.the_date desc 