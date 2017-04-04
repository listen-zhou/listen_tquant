# 均线数据和日K数据量对比结果查询，语气，单只股票的日K总量=单只股票均线数据总量+ma-1，如果是这样就有问题
select c.security_code, c.exchange_code from (
select a.security_code, a.exchange_code, a.k_count,
 b.avg_count ,b.ma, (a.k_count-b.avg_count + 1) diff
from (
select security_code, exchange_code, count(*) k_count from tquant_stock_day_kline
group by security_code, exchange_code) a
left join
(
select security_code, exchange_code, ma, count(*) avg_count from tquant_stock_average_line
group by security_code, exchange_code, ma
)b
on a.security_code = b.security_code
and b.exchange_code = a.exchange_code
having (a.k_count-b.avg_count + 1) != b.ma
) c
group by c.security_code, c.exchange_code;


--------------------------
# 查询不符合处理涨跌幅均的数据有哪些
select security_code, ma, count(*) from tquant_stock_average_line
where amount_avg_chg  is null
or vol_avg_chg is null
or price_avg_chg is null
or amount_flow_chg is null
or vol_flow_chg is null
group by security_code, ma
# 把涨跌幅均数据清空，重新跑
-- update tquant_stock_average_line set amount_avg_chg_avg = null
and vol_avg_chg_avg = null
and price_avg_chg_avg = null
and amount_flow_chg_avg = null
and vol_flow_chg_avg = null

# 检验涨跌幅均数据是否完整
select security_code, ma, count(*)
from (
select security_code, exchange_code, ma
 from tquant_stock_average_line
where ma = 3
and amount_avg_chg_avg  is null
and vol_avg_chg_avg is null
and price_avg_chg_avg is null
and amount_flow_chg_avg is null
and vol_flow_chg_avg is null
) b
group by security_code, ma
having count(*) > (ma -1)
;