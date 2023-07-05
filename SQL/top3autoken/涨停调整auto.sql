--SCS买点4：涨停调整
--买点描述：股价在近期支撑位出现大阴线破位走势，第二天大阳能强势收复。 肉眼可见前期有一波淋漓的上涨
 -----------------------------------------------------------------------------------
    
USE stock 
go 

--SELECT    ROW_NUMBER() OVER( PARTITION BY code ORDER BY riqi ASC) AS riqihao,*
--INTO T90
--FROM     dbo.lishijiager
--WHERE  riqi >='2023-05-19' and riqi <='2023-06-26'

DECLARE @i INT;
SET @i = (SELECT COUNT(1) FROM  dbo.T90 WHERE code = 'sz.000001')
WHILE (@i > 5) 
	BEGIN
	SELECT @i  
		;WITH T AS (
				SELECT riqihao,pctChg AS zhangdie,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],[kai],[shou],[di],[gao],[chengjiaoliang],[pctChg] 
				FROM     dbo.T90   
				WHERE    riqihao <= @i  )
			,T2 AS (
				SELECT *
				FROM T 
				WHERE zhangdie>=9.94
			)
			--SELECT * FROM T2
			,T5 AS (
		  		--见涨停价 后续价格数据中所有阴阳线 并统计后续阴阳线的数量
                SELECT COUNT(1) OVER (PARTITION BY T.code) AS zhangdiezhouqishu,T2.code,T2.riqi AS kaishiriqi,T.riqi,
                       T.riqihao,T.shitifudu,T.zhangdie,T2.riqihao AS zhangtingjiariqihao,T.riqihao AS zuihouriqihao,
                       T.di,T.kai,T.shou,T.gao,MIN(T.shitifudu) OVER (PARTITION BY T.code) AS zuidadiefu
                FROM  T2 INNER JOIN T ON T2.code = T.code AND T2.riqihao < T.riqihao
                )
				--SELECT * FROM T5 
			,T10 AS (
				--查找后续阴线 并且是次最后一个交易日期
                SELECT   *
                FROM     T5
                WHERE  zhangdie<0 AND T5.zuihouriqihao= (T5.zhangtingjiariqihao+zhangdiezhouqishu))
				--SELECT * FROM T10 
			,T13 AS ( 
				SELECT T10.kaishiriqi AS zhangtingriqi,T10.code,T10.riqi AS ciriqi,T.riqi AS zhuriqi,T10.shitifudu,
				T.di AS zhudi,T10.riqihao as ciriqihao,T10.zhangdie,T10.di,T10.kai,T10.shou,T10.gao,T10.zhangtingjiariqihao
                FROM T10 INNER JOIN T ON T10.code = T.code
                WHERE    T10.zuihouriqihao  = t.riqihao
				 AND T10.zhangdie < 0 
				AND ABS(T10.shitifudu)/(100/38.2) < T.shitifudu
                       AND T10.di * 1.03 >= T10.shou 
					   AND (T.di * 1.01 >= T.kai  OR (T.di * 1.03 >= T.kai AND T.shou>T10.gao/2 ) OR T10.di=T10.shou OR T.kai=T.di OR  T.shou>T10.gao )
					   )	
					--	SELECT * FROM T13 	
			,T15 AS ( 
				SELECT T13.ciriqihao,T13.code,T13.ciriqi,T13.zhangtingriqi,T13.zhangtingjiariqihao,T13.di AS cidi,T13.kai AS  cikai,T13.shou AS cishou,T13.gao AS cigao
			    FROM T13)		 
			,T16 AS (
				SELECT ROW_NUMBER() OVER (PARTITION BY T.code ORDER BY di ASC) AS RowID2, T.*,T15.ciriqi,T15.ciriqihao,T15.zhangtingriqi,T15.zhangtingjiariqihao,ciriqihao- zhangtingjiariqihao AS riqicha,cidi,cikai,cishou,cigao
				FROM T INNER JOIN T15 ON T15.code = T.code
				)
				SELECT zhangtingriqi,code,ciriqi,ciriqihao,zhangdie,cidi,cikai,cishou,cigao,zhangtingjiariqihao,riqicha
				INTO T9000
				FROM T16
				WHERE RowID2=1 AND (T16.zhangtingjiariqihao+8 = T16.ciriqihao OR T16.zhangtingjiariqihao+7 = T16.ciriqihao OR T16.zhangtingjiariqihao+6 = T16.ciriqihao OR T16.zhangtingjiariqihao+5 = T16.ciriqihao OR T16.zhangtingjiariqihao+4 = T16.ciriqihao OR T16.zhangtingjiariqihao+3 = T16.ciriqihao  )
 
 
					
       SET @i = @i - 1;	
	END
