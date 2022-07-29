--永远的N N字消化：上涨6-7天，回调4-5天比较好。  时间周期13天为个周期
 -- riqi >='2021-11-09' AND  riqi<='2021-11-29' 今天是2021-11-29 高点调整4-5 天   调整日期前是7个交易日是有连阳的
 -----------------------------------------------------------------------------------
   use stock 
   go 
;WITH    T AS ( SELECT       ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi asc  ) AS riqihao ,
                         ROW_NUMBER() OVER(PARTITION BY code ORDER BY gao DESC ) AS maxplusgao,
                        [code] ,
                        [riqi] ,
                        [kai] ,
                        [shou] ,
                        [di] ,
                        [gao] ,
                        [chengjiaoliang] ,
                         [pctChg] AS zhangdie
               FROM     dbo.lishijiager
			   WHERE    riqi >='2021-12-07' AND  riqi<='2021-12-23'
		     -- AND  code LIKE '%603256%'
             )-----------------------------------------------------------------
	,   T2
          AS (
		  --取最大值时间  最小值时间
		   SELECT  
		                 ROW_NUMBER() OVER ( PARTITION BY code ORDER BY gao DESC ) AS maxriqinum ,
						ROW_NUMBER() OVER ( PARTITION BY code ORDER BY di Asc ) AS minriqinum ,
                        *
						--点见高点开始日期第一天开始涨 涨7天后见高点
               FROM     T WHERE    riqi >='2021-12-08' AND  riqi<='2021-12-16'
             )
			-- select * from T2
       ,T3 AS (	SELECT * FROM T2  WHERE minriqinum=1)
	    ,T4 AS (SELECT * FROM T2  WHERE maxriqinum=1)
 
		,T5 AS (
		 SELECT T3.riqihao AS minriqihao ,T3.riqi AS minriqi ,T4.riqihao AS maxriqihao, T4.riqi AS maxriqi,T3.code FROM T3 INNER JOIN  T4 ON T3.code = T4.code  WHERE T3.riqihao<T4.riqihao)
-- SELECT * FROM T5
		 ,T6 AS (
		 SELECT (COUNT(1) OVER(PARTITION BY T5.code)) AS zongshunum, minriqi,maxriqi, T.* FROM T INNER JOIN  T5 ON T.code = T5.code WHERE T.riqihao>=T5.minriqihao AND T.riqihao<=maxriqihao
		 )
	  -- SELECT * FROM T6
		 ,T7 AS (
		 --阳的天数
		 SELECT (COUNT(1) OVER(PARTITION BY code)) as yangnum,   * FROM T6 WHERE  zhangdie>0)
		-- SELECT * FROM T7
		 , T8 AS (
		 SELECT DISTINCT SUM(zhangdie) OVER(PARTITION BY code) AS sumzf, MIN(riqihao) OVER(PARTITION BY code) AS minriqihao, MAX(riqihao) OVER(PARTITION BY code) AS maxriqihao, MIN(di) OVER(PARTITION BY code) AS mindi, MAX(gao) OVER(PARTITION BY code) AS maxgao, yangnum,zongshunum,minriqi,maxriqi,code   FROM T7 
		  WHERE 1=1 
		  --高点在最近5天出现
		  AND maxriqi='2021-12-16 00:00:00.000' 
		 and  zongshunum=7 
		 )
		 --T8是 N字的一撇找到了
		 --SELECT * FROM T8 ORDER BY sumzf DESC 
		 --T9 inner join T N字的一腊找到了
		 ,T9 as (
		 SELECT  (T.riqihao-maxriqihao) AS huitiaoshu, T8.*,SUM(T.zhangdie) OVER(PARTITION BY T.code) as sumdf, T.riqi,T.shou,T.di,T.kai,T.zhangdie
		  FROM T8  INNER JOIN  T ON T8.code = T.code 
		  INNER JOIN  T as A  ON T.code = A.code  and  A.maxplusgao=1 and T8.maxriqihao=A.riqihao
		  WHERE T8.maxriqihao<T.riqihao
		  )
		  SELECT  * FROM T9 WHERE 1=1   
		AND riqi='2021-12-23' 
		--AND kai/1.03<di   
		-- AND ((maxgao-mindi)/(100/62.5))+mindi<di
		 ORDER BY sumzf DESC
		 

	 