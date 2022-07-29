----SCS买点2：绝地反击
---买点描述：股价在连续下跌后收出低位大阴线（光脚）形态，第二天股价低开后高走，盘间最高价回补缺口
---最终低开收阳的倒锤子形态
 
 -----------------------------------------------------------------------------------
 --找最近60个交易日见高点后下跌23天的K线
   use stock 
   go 
WITH    T AS ( SELECT   
 ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi desc) AS riqidaoxu,
 ROW_NUMBER() OVER(PARTITION BY code ORDER BY gao desc) AS gaodaoxu,
 [code] ,
                        [riqi] ,
                        [kai] ,
                        [shou] ,
                        [di] ,
                        [gao] ,
                
                         [pctChg] AS zf						
						 
               FROM     dbo.lishijiager
			   --60个交易日
 WHERE    riqi >='2021-11-22' AND  riqi<='2021-12-15'
							)
,T2  AS (  --高点在最近23天出现
							SELECT * FROM T WHERE gaodaoxu=1 AND riqi='2021-11-25')
							,T3 AS (
							--高点后续
							SELECT T.* FROM T2 INNER JOIN T  ON T2.code = T.code  WHERE T2.riqidaoxu>T.riqidaoxu)

							SELECT  * FROM T3 AS T  INNER JOIN T3 AS T0   ON T.code = T0.code 
							WHERE T.riqidaoxu=T0.riqidaoxu-1   
							--前一天进似光脚阴线 
							AND T0.di*1.01>=T0.shou AND T0.zf<0
							AND T0.riqidaoxu=2
							-- 后一天进似光脚倒锤子
							AND T.kai<=T0.di  
							AND ((T.kai*1.03>T.shou AND T.zf>0) OR  ( T.di*1.03>T.shou AND  T.zf<0) )
							ORDER BY T.shou desc


		 		
		  