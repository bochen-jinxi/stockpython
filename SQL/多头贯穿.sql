--AND T.riqi='2021-11-05' 日期多头贯穿
 -----------------------------------------------------------------------------------
  use stock 
   go 
WITH    T AS ( SELECT   
 ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi desc) AS riqidaoxu,
 ROW_NUMBER() OVER(PARTITION BY code ORDER BY gao desc) AS gaodaoxu,
  ROW_NUMBER() OVER(PARTITION BY code ORDER BY [pctChg] asc) AS zfzhengxu,
 [code] ,
                        [riqi] ,
                        [kai] ,
                        [shou] ,
                        [di] ,
                        [gao] ,
                
                         [pctChg] AS zf						
						 
               FROM     dbo.lishijiager
 WHERE    riqi >='2021-12-20' AND  riqi<='2021-12-24'
							)

							SELECT  * FROM T   INNER JOIN T AS T0   ON T.code = T0.code 
							WHERE T.riqidaoxu=T0.riqidaoxu-1   
							--前一天阴
							AND (T0.di*1.03)>=T0.shou AND T0.zf<0
							-- 前一天跌幅最大
							AND T0.zfzhengxu=1
							
							-- 后一天至少低开一个点 收贯穿
							AND T.kai*1.01<=T0.di   AND T.shou>=(T0.shou-T0.kai)/(100/50)+T0.kai 
							AND T.zf>=0
							AND T.riqi='2021-12-24'
							ORDER BY T0.zf 

		 		
		  


	
		 
  