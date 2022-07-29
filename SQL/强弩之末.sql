---SCS买点1：强弩之末
--买点描述：在下跌过程中连续出现几根影线长,体小的K线后，股价在低位收出大实体阳线。
---意味着下跌趋势将被扭转。
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
             --  WHERE    riqi >= DATEADD(DAY, -21, GETDATE())
			    WHERE    riqi >='2021-10-25' AND  riqi<='2021-11-04'
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
						T3.di
               FROM     T4
                        INNER JOIN T3 ON T4.code = T3.code
                                         AND T4.riqi < T3.riqi
               WHERE    T4.RowID = 1
             ),
        T6
          AS (
		  -- 后续数据按日期正序标号  最低价按日期倒序标号
		   SELECT   ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi ) AS riqihao ,
		   ROW_NUMBER() OVER ( PARTITION BY code ORDER BY di DESC ) AS zuidijiahao ,
                        *
               FROM     T5
             ),
        T7
          AS (
		  --查找后续中所有阴线并重新按日期正序标号 用以查找连续日期号的阴线
		   SELECT   riqihao
                        - ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi ) AS lianxuxiadieriqizu ,
                        *
               FROM     T6
               WHERE    --code='sz.300983' AND   
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
             ),
        T10
          AS ( 
		  	 --查找后续中所有阴线中 连续下跌天数最大 并且 上/下影线是实体的2倍的代码
		  SELECT   COUNT(1) OVER ( PARTITION BY code ) AS lianxuxiadeshangyingxiashu ,
                        *
               FROM     T9
               WHERE    zuidalianxuxiadieshu = lianxuxiadieshu
                        AND ( syxbst > 2  OR xyxbst > 2)
             )
			 --T6 大阳线
    SELECT  DISTINCT   T10.code, T10.kaishiriqi,
            T6.riqi
    FROM    T10
            INNER JOIN T6 ON T10.code = T6.code
    WHERE   T6.zhangdie = 1
            AND T6.shitifudu >1 
            AND T6.riqihao = T6.zhangdiezhouqishu
			--AND  T6.zuidijiahao=T6.zhangdiezhouqishu
           AND lianxuxiadeshangyingxiashu > 2
		   AND  T6.riqi='2021-11-04'
 
	
	
  

