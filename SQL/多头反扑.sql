--AND T.riqi='2021-11-05' 日期多头反扑
 -----------------------------------------------------------------------------------
   use stock 
   go 
WITH    T AS ( SELECT   
 ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi desc) AS riqidaoxu,
 ROW_NUMBER() OVER(PARTITION BY code ORDER BY gao desc) AS gaodaoxu,
  ROW_NUMBER() OVER(PARTITION BY code ORDER BY di asc) AS dizhengxu,
 [code] ,
                        [riqi] ,
                        [kai] ,
                        [shou] ,
                        [di] ,
                        [gao] ,
                
                         [pctChg] AS zf						
						 
               FROM     dbo.lishijiager
 WHERE    riqi >='2021-12-20' AND  riqi<='2021-12-23'
							)

							SELECT  * FROM T   INNER JOIN T AS T0   ON T.code = T0.code 
							WHERE T.riqidaoxu=T0.riqidaoxu-1   
							--前一天阴
							AND (T0.di*1.03)>=T0.shou AND T0.zf<0
							--AND T0.riqidaoxu=2
							-- 后一天反扑
							AND T.kai>T0.shou   AND T.shou>T0.kai
							AND T.zf>=0
							AND T.riqi='2021-12-23'
							AND T0.dizhengxu=1
							ORDER BY T0.zf 