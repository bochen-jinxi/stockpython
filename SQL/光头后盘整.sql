---	WHERE tingriqi='2021-10-22' 后续天数不破涨停2分之一水位
 -----------------------------------------------------------------------------------

USE stock 
   go 
WITH    T AS ( SELECT      
						   ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi DESC ) AS riqihao ,
						    ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi ASC ) AS riqihao2 ,
                        [code] ,
                        [riqi] ,
                        [kai] ,
                        [shou] ,
                        [di] ,
                        [gao] ,
                        [chengjiaoliang] ,
                         [pctChg] AS zf
               FROM     dbo.lishijiager
              WHERE    riqi >= '2021-12-13'                        AND riqi <= '2021-12-24'
						--AND  code NOT  LIKE 'sh.688%'
	--AND   code='sz.000811'
                        
	         )
			 ,T2 AS (
			 -------找到最早光头  日期 最低的收盘价格 最低的日期 统计涨停的次数
	 SELECT DISTINCT MIN(riqihao2) OVER(PARTITION BY code) AS tingriqihao2,  MIN(shou) OVER(PARTITION BY code) AS guangtoushou, COUNT(1) OVER(PARTITION BY code) AS tongjitingtianshu,   code  FROM  T   WHERE 
	 
	   1=1
	 	AND T.gao=T.shou   AND (( T.zf<9.94    AND  T.code  NOT  LIKE 'sz.300%') OR ( T.zf<19.90  AND  T.code  LIKE 'sz.300%')	)  AND T.code   NOT  LIKE 'sh.688%'  )

	
 ,T3 AS (
 -- 后续的天数
	SELECT COUNT(1) OVER(PARTITION BY T2.code) AS houxutianshu, tingriqihao2, tongjitingtianshu,
	
(SELECT TOP 1 kai FROM T AS A WHERE A.code=T.code AND guangtoushou=A.shou ) AS guangtoukai,
	
	guangtoushou ,T.* FROM T2 INNER JOIN  T ON T2.code = T.code WHERE T2.tingriqihao2<T.riqihao2   
	)
	,T4 AS (
	--后续最低价不破最早光头实体2分之1
	SELECT COUNT(1) OVER(PARTITION BY T3.code) AS houxuzhongtianshu,* FROM T3  WHERE 1=1 AND 
	  (guangtoushou-guangtoukai)/(100/50)+guangtoukai<T3.shou
	 --下影线在3个点内
	AND  ( (di*1.03>=shou AND zf<0) 	OR (di*1.03>=kai AND zf>=0) )
		) 
   --后续调整不破2分之一水位
   ,T5 AS (
	SELECT (SELECT TOP 1 riqi FROM T WHERE T.code=T4.code AND T.riqihao2=T4.tingriqihao2) AS tingriqi,
	 (SELECT COUNT(1) FROM T4 AS A WHERE T4.code=A.code AND zf>=0) AS yangtianshu,
	houxuzhongtianshu-(SELECT COUNT(1) FROM T4 AS A WHERE T4.code=A.code AND zf>=0) AS yintianshu,
	 *    FROM T4 WHERE  houxuzhongtianshu=houxutianshu )
	
	SELECT * FROM T5	
	WHERE tingriqi='2021-12-14' 
	AND yangtianshu>yintianshu
	ORDER BY shou DESC ,riqi DESC ,code
	-- houxutianshu IN(4,5,6,7,8)    


	
		 
  