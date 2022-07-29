---N字结构 见高点后下跌4天之内见地点 有破底N字结构
 -----------------------------------------------------------------------------------
 --找最近8个交易日的K线键高点的
USE stock 
   go
  -- SET STATISTICS IO ON  
;WITH    T AS ( SELECT   [pctChg] AS zhangdie ,
                        [code] ,
                        [riqi] ,
                        [kai] ,
                        [shou] ,
                        [di] ,
                        [gao] ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi ASC ) AS dateasc ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi DESC ) AS datedesc ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY gao DESC ) AS maxgao ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY di ASC ) AS mindi
               FROM     dbo.lishijiager
               WHERE    riqi >= '2021-11-15'
                        AND riqi <= '2021-11-25'
                    -- AND code LIKE '%300405%'
             )--最高点日期
        ,
        T2
          AS ( SELECT   *
               FROM     T
               WHERE    maxgao = 1
             )-- SELECT * FROM T2
			 ,
			  
			 
			 ----最低点日期
    --    ,T3
    --      AS ( SELECT   *
    --           FROM     T
    --           WHERE    mindi = 1
    --         ),
			 --	 见高点后下跌8天之内见地点
        T4
          AS ( SELECT   
                        T2.dateasc AS griqi ,
                        T2.gao AS zuigao ,
                        T2.riqi AS zuigaoriqi ,
                        MIN(T.di) OVER ( PARTITION BY T.code ) AS di1jiage ,
                        T.*
               FROM     T2
                        INNER JOIN T ON T2.code = T.code
               WHERE    ( T.dateasc - 1 = T.dateasc
                          OR T.dateasc - 2 = T2.dateasc
                          OR T.dateasc - 3 = T2.dateasc
                          OR T.dateasc - 4 = T2.dateasc
                          OR T.dateasc - 5 = T2.dateasc
                          OR T.dateasc - 6 = T2.dateasc
                          OR T.dateasc - 7 = T2.dateasc
                          OR T.dateasc - 8 = T2.dateasc
                        )
                        AND T.zhangdie < 0
             )
			 --SELECT * FROM T4
			 --1低点日期 日期倒序排号
      ,T40 AS (
	  SELECT T4.* FROM T4  INNER JOIN  T4 AS A  ON  T4.code = A.code
	  WHERE T4.di=a.di1jiage
	   ) 
	   --SELECT * FROM T40
	   ,T400 AS (
	  SELECT   T40.dateasc - A.dateasc AS d1xiadietianshu , T40.* FROM T40  INNER JOIN  T AS A  ON  T40.code = A.code
	   
	   )
	    ,
        T5
          AS ( SELECT   MAX(d1xiadietianshu) OVER ( PARTITION BY code ) AS jin8tianxiadieshu ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY dateasc DESC ) AS d1dataasc ,
                        *
               FROM     T400
             ),
			 -- 1低点最后日期
        T6
          AS ( SELECT   jin8tianxiadieshu ,
                        zuigao ,
                        zuigaoriqi ,
                        di1jiage ,
                        code ,
                        riqi ,
                        dateasc ,
                        datedesc
               FROM     T5
               WHERE    d1dataasc = 1
             ),
			 --1低点后反弹高点
        T7
          AS ( SELECT   MAX(T.gao) OVER ( PARTITION BY T.code ) AS fantangao ,
                        jin8tianxiadieshu ,
                        zuigao ,
                        zuigaoriqi ,
                        di1jiage ,
                        T.gao AS houxugao , 
						 T.di AS houxudi ,
                        T6.riqi AS di1qiri ,
                        T.code ,
                        T6.dateasc ,
                        T6.datedesc ,
                        T.riqi AS houxuqiri
               FROM     T6
                        INNER JOIN T ON T6.code = T.code
               WHERE    T6.datedesc > T.datedesc
             )
			--SELECT * FROM T7
			 ,T8  AS (
			 ------------------反弹至前高1半水位 最终破1底点
    SELECT     zuigaoriqi,zuigao,di1qiri, code, ( SELECT TOP 1
                      riqi
              FROM      T AS A
              WHERE     A.code = T7.code
                        AND A.gao = T7.fantangao
            ) AS fantangaoriqi ,
			MAX(houxuqiri) OVER(PARTITION BY  T7.code) AS zuidadi2riqi,
			( SELECT TOP 1
                      zhangdie
              FROM      T AS A
              WHERE     A.code = T7.code
                        AND A.datedesc=1
            ) AS dangtianzhangfu ,
			( SELECT TOP 1
                      shou
              FROM      T AS A
              WHERE     A.code = T7.code
                        AND A.datedesc=1
            ) AS dangtianshou ,
			 ((T7.zuigao-di1jiage)/2)+di1jiage AS c2o,
          fantangao,jin8tianxiadieshu,
		  COUNT(1) OVER(PARTITION BY  T7.code) AS po1ditianshu
    FROM    T7    WHERE  ((T7.zuigao-di1jiage)/2)+di1jiage <=T7.fantangao AND T7.di1jiage>houxudi
	 )
	 --SELECT * FROM T8
	 -----------------
	 SELECT DISTINCT * FROM  T8 WHERE  zuidadi2riqi>fantangaoriqi  
	 AND  fantangaoriqi>di1qiri
	AND  dangtianzhangfu>0 
	AND  zuidadi2riqi='2021-11-25 00:00:00.000'
	 ORDER BY dangtianshou DESC 
	 



 


	
		 
  