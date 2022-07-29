 -- AND T.riqi='2021-11-08' 日期 阳包阳
 
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
 WHERE    riqi >='2021-10-01' AND  riqi<='2021-11-25'
							)

							SELECT  * FROM T   INNER JOIN T AS T0   ON T.code = T0.code 
							WHERE T.riqidaoxu=T0.riqidaoxu-1   
							--前一天进似2个点下影线
							AND (T0.di*1.02)>=T0.kai
							 AND T0.zf>=0
							AND T0.riqidaoxu=2
							-- 后一天反包
							AND T.kai<=T0.di
							--   AND T.shou >=T0.shou  
							 AND T.zf>=0
							AND T.riqi='2021-11-25'
						
							ORDER BY T.shou desc


		 		
		  