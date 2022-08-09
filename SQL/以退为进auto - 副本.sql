--SCS买点2：以退为进
--买点描述：股价在连续下跌后收出低位锤子形态，第二天股价低开后高走，最终收出实体大影线小的阳线，
---并且收盘价格突破头天锤子线最高价
 
 -----------------------------------------------------------------------------------
  --找最近8个交易日的K线

    use stock 
   go 

    --SELECT    ROW_NUMBER() OVER( PARTITION BY code ORDER BY riqi ASC) AS riqihao,*
    --          INTO T90
			 --  FROM     dbo.lishijiager
			 -- WHERE  riqi >='2022-04-01' 

			 DECLARE @i INT ;
			 SET @i=(SELECT COUNT(1) FROM dbo.T90 WHERE  code ='sz.000001')
			 WHILE(@i>5)
			 BEGIN
       

  WITH    T AS ( SELECT   pctChg AS zhangdie ,
                        ( shou - kai ) AS shiti ,
                        ( shou - kai ) / kai * 100 AS shitifudu ,
                        [code] ,
                        [riqi] ,
						 [riqihao] AS  [riqihao0] ,
                        [kai] ,
                        [shou] ,
                        [di] ,
                        [gao] ,
                        [chengjiaoliang] ,
                         [pctChg]
                FROM dbo.T90 WHERE riqihao<=@i
             
             )-----------------------------------------------------------------
 ,      T2
          AS (
		   --取上/下影线
		   SELECT   ( CASE 
                            WHEN zhangdie>0 THEN ( gao - shou )
                            WHEN zhangdie<=0 THEN ( kai - gao )
                          END ) AS shanyingxian ,
                        ( CASE  
                            WHEN zhangdie>0 THEN ( kai - di )
                            WHEN zhangdie<=0 THEN ( di - shou )
                          END ) AS xiayingxian ,
                        *
               FROM     T
             )----------------------------------------------------------------
	,   T3
          AS (
		    --冲高回落/探底回升的比例 幅度
		   SELECT   shanyingxian / shiti AS syxbst ,
                        xiayingxian / shiti AS xyxbst ,
						( CASE  
                            WHEN  zhangdie>0 THEN shanyingxian/shou
                            WHEN zhangdie<=0 THEN shanyingxian/kai
                          END ) AS shangyingxianfudu ,
                        ( CASE  
                            WHEN  zhangdie>0 THEN xiayingxian/kai
                            WHEN zhangdie<=0 THEN xiayingxian/shou
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
						T3.riqihao0 ,
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
		AND (ABS(shangyingxianfudu))*100<=0.5		
		-- AND zhangdie=1
		 )
		 	INSERT INTO  [dbo].[T902](
	[riqihao0] ,
	[riqi] ,
	[code] ,
	[kaishiriqi] ,
	[zhongjianriqi]
) 		 
		 SELECT T6.riqihao0,T6.riqi,T7.code,T7.kaishiriqi,T7.riqi AS zhongjianriqi
		--INTO T902
		  FROM T7 INNER JOIN T6 ON  T7.code = T6.code
		 WHERE  T7.riqihao+1=T6.riqihao
		  AND T7.shou>T6.kai 
		  AND T7.gao<T6.shou
		AND  T6.riqihao0=@i
		 ORDER BY T7.code
		 		
  

  	SET @i=@i-1;
			 END