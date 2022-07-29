--AND T.riqi='2021-11-05' 日期多头母子
 -----------------------------------------------------------------------------------
 --找最近8个交易日的K线
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
 WHERE    riqi >='2021-12-06' AND  riqi<='2021-12-16'
							)

							SELECT  * FROM T   INNER JOIN T AS T0   ON T.code = T0.code 
							WHERE T.riqidaoxu=T0.riqidaoxu-1   
							--前一天阴
							AND (T0.di*1.03)>=T0.shou AND T0.zf<0
							--AND T0.riqidaoxu=2
							-- 后一天孕
							AND T.kai>=T0.shou   AND T.gao<=T0.kai AND T.di>=T0.shou
							AND T.zf>=0
							AND T.riqi='2021-12-16'
							ORDER BY T0.zf 


		 		
		  