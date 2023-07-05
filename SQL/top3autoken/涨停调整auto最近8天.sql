--SCS买点4：涨停调整 最近8天有涨停 切最后一天不是涨停 光脚阴线在均线附件
--买点描述：股价在近期支撑位出现大阴线破位走势，第二天大阳能强势收复。 肉眼可见前期有一波淋漓的上涨
 -----------------------------------------------------------------------------------
    
USE stock 
go 

--SELECT    ROW_NUMBER() OVER( PARTITION BY code ORDER BY riqi ASC) AS riqihao,*
--INTO T90
--FROM     dbo.lishijiager
--WHERE  riqi >='2023-06-19' and riqi <='2023-07-03'

DECLARE @i INT;
SET @i = (SELECT COUNT(1) FROM  dbo.T90 WHERE code = 'sz.000001')

--WHILE (@i > 5) 
	--BEGIN
	--SELECT @i  
		;WITH T AS (
				SELECT riqihao,pctChg AS zhangdie,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],[kai],[shou],[di],[gao],[chengjiaoliang],[pctChg] 
				FROM     dbo.T90   
				WHERE    riqihao >= @i-8 AND  riqihao<=@i  )
			,T2 AS (
				SELECT *
				FROM T 
				WHERE zhangdie>=9.94 AND riqihao!=@i
			)
			--SELECT * FROM T2
			,T5 AS (
		  		--见涨停价 后续价格数据中所有阴阳线 并统计后续阴阳线的数量
                SELECT COUNT(1) OVER (PARTITION BY T.code) AS zhangdiezhouqishu,T2.code, T2.code AS code1,T2.riqi AS kaishiriqi,T.riqi,
                       T.riqihao,T.shitifudu,T.zhangdie,T2.riqihao AS zhangtingjiariqihao,T.riqihao AS zuihouriqihao,
                       T.di,T.kai,T.shou,T.gao,MIN(T.shitifudu) OVER (PARTITION BY T.code) AS zuidadiefu
                FROM  T2 INNER JOIN T ON T2.code = T.code AND T2.riqihao < T.riqihao 
                )
				--SELECT * FROM T5 
			,T10 AS (
				--查找后续K线 并且是最后一个交易日期不是涨停板的
                SELECT   *
                FROM     T5 
                 
				)

				---SELECT  * FROM  T10
				 SELECT DISTINCT '1900-01-01' AS zhangtingriqi,code,  '1900-01-01' AS ciriqi
				 				 				INTO T9090 
								FROM T10 
			 
					
      -- SET @i = @i - 1;	
	--END
