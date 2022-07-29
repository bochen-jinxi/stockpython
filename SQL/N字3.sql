---N字结构 见高点后下跌4天之内见地点 2浪N字结构
 -----------------------------------------------------------------------------------
 --找最近8个交易日的K线键高点的
USE stock 
   go 
WITH    T AS ( SELECT   ( CASE WHEN ( shou - kai ) > 0 THEN 1
                               WHEN ( shou - kai ) = 0 THEN 0
                               WHEN ( shou - kai ) < 0 THEN -1
                          END ) AS zhangdie ,
						    ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi ASC ) AS riqihao2 ,
                        [code] ,
                        [riqi] ,
                        [kai] ,
                        [shou] ,
                        [di] ,
                        [gao] ,
                        [chengjiaoliang] ,
                        1 AS [pctChg]
               FROM     dbo.lishijiager
               WHERE    riqi >= '2021-10-01'
                        AND riqi <= '2021-11-10'
--	AND code like '%sh.600057%'
                        
             )-----------------------------------------------------------------
	,   T2
          AS (
		  --取最大值时间  最小值时间 实体幅度 上影线和实体的比值 上影线幅度
               SELECT   ( CASE zhangdie
                            WHEN 1 THEN ( shou - kai ) / kai
                          END ) * 100 AS shitifudu ,
                        ( ( CASE zhangdie
                              WHEN 1 THEN ( gao - shou )
                            END ) / ( CASE zhangdie
                                        WHEN 1 THEN ( shou - kai )
                                      END ) ) AS syxbst ,
                        ( CASE zhangdie
                            WHEN 1 THEN ( gao - shou ) / shou
                          END ) * 100 AS syxfd ,
                      
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi DESC ) AS riqihao ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY gao DESC ) AS RowID ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY di ASC ) AS RowID2 ,
                        *
               FROM     T
             ),
			 --最高点日期
        T3
          AS ( SELECT   *
               FROM     T2
               WHERE    RowID = 1
                        AND zhangdie = 1
                                             
             ),
		
			 --	 见高点后下跌4天之内见地点
        T5
          AS ( SELECT   T2.riqihao2 - T3.riqihao2 AS num ,
                        T3.riqi AS griqi ,
                        T2.riqi AS d1riqi ,
                        T2.* ,
                        T2.di AS d1jiage
               FROM     T3
                        INNER JOIN T2 ON T3.code = T2.code
               WHERE    ( T2.riqihao2 - 1 = T3.riqihao2
                          OR T2.riqihao2 - 2 = T3.riqihao2
                          OR T2.riqihao2 - 3 = T3.riqihao2
                          OR T2.riqihao2 - 4 = T3.riqihao2
                        )
                        AND T2.zhangdie = -1
             ),
		
			 --1低点日期 日期倒序排号
        T6
          AS ( SELECT   MAX(num) OVER ( PARTITION BY code ) AS jin4tianxiadieshu ,
                        MIN(d1jiage) OVER ( PARTITION BY code ) AS d1jia ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqihao2 DESC ) AS num3 ,
                        *
               FROM     T5
             )
			  -- 1低点最大日期
        ,T7
          AS ( SELECT   *
               FROM     T6
               WHERE    num3 = 1
             )
			 ,T8 AS (
			 	 	 --最低价大于4天内最低价 日期在后续日期的
      SELECT T7.griqi,T7.d1riqi,T2.code, T2.riqi AS lang2riqi,T2.shou,
	   T2.riqihao2 AS lang2riqihao2,T2.di AS lang2djia, T7.jin4tianxiadieshu,T7.d1jia,(MAX(T2.riqihao2) OVER(PARTITION BY T2.code) -T7.riqihao2)  AS chariqihao2,  count(T2.riqihao2) OVER(PARTITION BY  T2.code) AS countriqihao2           FROM     T2 INNER JOIN  T7 ON T2.code = T7.code
               WHERE        T2.di>d1jia AND  T2.riqihao2>T7.riqihao2
           
			)
			,T9 AS (
			SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY lang2riqihao2 desc ) AS num4, * FROM T8 WHERE chariqihao2=countriqihao2
			)
			SELECT * FROM T9 WHERE num4=1
			  --2低点破1低点 1底点 和 2底点之间低点间距3个点之内
                 AND      ( ( lang2djia - d1jia ) / d1jia ) * 100 <3
				 AND lang2riqi='2021-11-10'
				 ORDER BY shou DESC 


 


	
		 
  