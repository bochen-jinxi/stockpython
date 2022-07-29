--SCS买点4：破低反涨
--买点描述：股价在近期支撑位出现大阴线破位走势，第二天大阳能强势收复。 肉眼可见前期有一波淋漓的上涨
 
 -----------------------------------------------------------------------------------
    --找最近8个交易日的K线
	  use stock 
   go 
WITH    T AS ( SELECT   ( CASE WHEN ( shou - kai ) > 0 THEN 1
                               WHEN ( shou - kai ) = 0 THEN 0
                               WHEN ( shou - kai ) < 0 THEN -1
                          END ) AS zhangdie ,
                        ( shou - kai ) AS shiti ,
                        ( shou - kai ) / kai * 100 AS shitifudu ,
                        [code] ,
                        [riqi] ,
                        [kai] ,
                        [shou] ,
                        [di] ,
                        [gao] ,
                        [chengjiaoliang] ,
                        1 AS [pctChg]
               FROM     dbo.lishijiager
              -- WHERE    riqi >= DATEADD(DAY, -21, GETDATE())
			  WHERE    riqi >='2022-03-01' AND  riqi<='2022-04-01'
			   --AND   code LIKE '%000090%'
             )-----------------------------------------------------------------
 ,      T2
          AS ( 
		    --取上/下影线
		  SELECT   ( CASE zhangdie
                            WHEN 1 THEN ( gao - shou )
                            WHEN -1 THEN ( kai - gao )
                          END ) AS shanyingxian ,
                        ( CASE zhangdie
                            WHEN 1 THEN ( kai - di )
                            WHEN -1 THEN ( di - shou )
                          END ) AS xiayingxian ,
                        *
               FROM     T
             )----------------------------------------------------------------
	,   T3
          AS ( 
		   --冲高回落/探底回升的比例 
		  SELECT   shanyingxian / shiti AS syxbst ,
                        xiayingxian / shiti AS xyxbst ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY gao DESC ) AS RowID ,
						
                        *
               FROM     T2
             ),
        T4
          AS ( 
		    -- 各代码见高点的日期 价格
		  SELECT   *
               FROM     T3
               WHERE    RowID = 1
             )-----------------------------------------------------------------------
	,   T5
          AS (
		  	--见高点后 后续价格数据中所有阴阳线 并统计后续阴阳线的数量
		   SELECT   COUNT(1) OVER ( PARTITION BY T3.code ) AS zhangdiezhouqishu ,
                        T4.code ,
                        T4.riqi AS kaishiriqi ,
                        T3.riqi ,
                        T3.shiti ,
                        T3.shitifudu ,
                        T3.zhangdie ,
                        T3.syxbst ,
                        T3.xyxbst,
						T3.di,
						T3.kai,
						T3.shou,
						T3.gao,
						MIN(T3.shitifudu) OVER(PARTITION BY T3.code) AS zuidadiefu
               FROM     T4
                        INNER JOIN T3 ON T4.code = T3.code
                                         AND T4.riqi < T3.riqi
               WHERE    T4.RowID = 1
             ),
        T6
          AS ( 
		      -- 后续数据按日期正序标号  最低价倒序
		  SELECT   ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi ) AS riqihao ,
		   ROW_NUMBER() OVER ( PARTITION BY code ORDER BY di DESC ) AS zuidijiahao ,
                        *
               FROM     T5
             ),
        T7
          AS (
		   --查找后续中所有阴线并重新按日期正序标号 用以查找连续日期号的阴线
		   SELECT   riqihao
                        - ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi ) AS lianxuxiadieriqizu , COUNT(1) OVER(PARTITION BY code) AS  yingxianshu,
						(SELECT COUNT(1) FROM T6 AS A WHERE A.zhangdie=1 AND A.code=T6.code ) AS yangxianshu,
                         *
               FROM     T6
               WHERE   --- code='sh.603985' AND   
                        zhangdie = -1
             ),
        T8
          AS (
		   --标识后续中所有连续阴线的天数
		   SELECT   COUNT(1) OVER ( PARTITION BY code, lianxuxiadieriqizu ) AS lianxuxiadieshu ,
                        *
               FROM     T7
             ),
        T9
          AS ( 
		   --标识后续中阴线最大连续天数 
		  SELECT   MAX(lianxuxiadieshu) OVER ( PARTITION BY code ) zuidalianxuxiadieshu ,
                        *
               FROM     T8
             )
			 ,T10 AS (
			 --查找后续最大跌幅的阴线 并且是上一个交易日期
			 SELECT * FROM T9 WHERE riqihao+1=zhangdiezhouqishu 
			-- AND  T9.shitifudu=zuidadiefu
			 		 			-- AND zuidijiahao=zhangdiezhouqishu
			 )
			 --SELECT * FROM T10
			--第二天阳线收复
		 SELECT DISTINCT T10.kaishiriqi,T10.code,T10.riqi,T6.riqi,  T10.shitifudu,T6.shitifudu,T10.yingxianshu,T10.yangxianshu, T10.* FROM T10 INNER JOIN T6 ON  T10.code = T6.code
		 WHERE  
	     T10.riqihao+1=t6.riqihao 
		 AND T10.zhangdie=-1  AND  ABS(T10.shitifudu)/(100/61.8)<T6.shitifudu
		  and T10.kai>=T6.shou	
	     AND (T10.zuidijiahao=T10.zhangdiezhouqishu	 OR T10.zuidijiahao+1=T10.zhangdiezhouqishu)
		 --AND T10.zhangdiezhouqishu>8	 	
		 AND T10.yingxianshu<T10.yangxianshu
			 	AND 		  T6.riqi='2022-04-01'	
				 ORDER BY T10.code
		  
 