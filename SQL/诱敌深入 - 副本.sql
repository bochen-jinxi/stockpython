-- SCS买点5：诱敌深入1
--买点描述：
--1.下跌波段中,收出锤子止跌形态后,股价继续下跌,在锤子下影线区域收出大阴大阳组合
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
               --WHERE    riqi >= DATEADD(DAY, -21, GETDATE())
			  WHERE     riqi >=dateadd(day,-1,'2021-10-25')  AND  riqi<='2021-11-04'
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
		    --冲高回落/探底回升的比例 幅度
		   SELECT   shanyingxian / shiti AS syxbst ,
                        xiayingxian / shiti AS xyxbst ,
						( CASE zhangdie
                            WHEN 1 THEN shanyingxian/shou
                            WHEN -1 THEN shanyingxian/kai
                          END ) AS shangyingxianfudu ,
                        ( CASE zhangdie
                            WHEN 1 THEN xiayingxian/kai
                            WHEN -1 THEN xiayingxian/shou
                          END ) AS xiayingxianfudu ,
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
						T3.shangyingxianfudu,
						T3.xiayingxianfudu,
						MIN(T3.shitifudu) OVER(PARTITION BY T3.code) AS zuidadiefu
               FROM     T4
                        INNER JOIN T3 ON T4.code = T3.code
                                         AND T4.riqi < T3.riqi
               WHERE    T4.RowID = 1
             ),
			  T6
          AS (
		    -- 后续数据按日期正序标号  标记下影线幅度
		   SELECT   ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi ) AS riqihao ,
		    MAX(ABS(xiayingxianfudu)) OVER(PARTITION BY code) AS zuidajueduizhixiayingxianfudu , 
			LAG(shou) OVER ( PARTITION BY code ORDER BY riqi ) AS  shou0,
			LAG(kai) OVER ( PARTITION BY code ORDER BY riqi ) AS  kai0,
			LAG(di) OVER ( PARTITION BY code ORDER BY riqi ) AS  di0,
			LAG(gao) OVER ( PARTITION BY code ORDER BY riqi ) AS  gao0,
                        *
               FROM     T5 			
             )
			 ,T7 AS  (
			 SELECT * FROM T6  
			 WHERE    --跳空低开0.1个点
		    (di0-kai)/kai*100>0.1
		AND (ABS(xiayingxianfudu))*100>2
		AND (ABS(shangyingxianfudu))*100<=0.5		-- AND zhangdie=1
		 )
		
					 , T8 AS (
			 --后续数据数据查找下影线最长的K线 标记下影线的区域范围
				SELECT  *,( CASE zhangdie
                           WHEN 1 THEN  kai 
                          WHEN -1 THEN  shou 
                         END ) AS xyxquyugao FROM T7
				)

				--查找最大下影线K线 后续中所有K线 收盘价收在下影线区域中 并且最低价在下影线区域中 收红盘实体大于3
					 SELECT T6.riqi AS zriqi, T6.zhangdie, T8.kaishiriqi, T8.riqi, T6.*   
			 FROM T8 INNER JOIN T6 ON  T8.code = T6.code
			 WHERE T8.riqihao<T6.riqihao 
		 AND  T8.xyxquyugao>T6.shou 
		AND  T8.di<=T6.di
	 AND T6.zhangdie=1 
	 	AND  T6.riqi= '2021-11-04'
	 --AND T6.shitifudu>1
			 ORDER BY code  
		 