--SCS买点4：破低反涨
--买点描述：股价在近期支撑位出现大阴线破位走势，第二天大阳能强势收复。 肉眼可见前期有一波淋漓的上涨
 -----------------------------------------------------------------------------------
    
USE stock 
go 

--SELECT    ROW_NUMBER() OVER( PARTITION BY code ORDER BY riqi ASC) AS riqihao,*
--INTO T90
--FROM     dbo.lishijiager
--WHERE  riqi >='2022-07-01' and riqi <='2022-08-08'

DECLARE @i INT;
SET @i = (SELECT COUNT(1) FROM  dbo.T90 WHERE code = 'sz.000001')
WHILE (@i > 5) 
	BEGIN
		WITH T AS (
				SELECT riqihao,pctChg AS zhangdie,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],[kai],[shou],[di],[gao],[chengjiaoliang],[pctChg] 
				FROM     dbo.T90
				WHERE    riqihao <= @i)
				--SELECT * FROM T	
			,T3 AS ( 	 
				SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY gao DESC) AS RowID,*
				FROM T)
			,T4 AS ( 
				-- 各代码见高点的日期 价格
                SELECT   *
                FROM     T3
                WHERE    RowID = 1 )
			,T5 AS (
		  		--见高点后 后续价格数据中所有阴阳线 并统计后续阴阳线的数量
                SELECT COUNT(1) OVER (PARTITION BY T3.code) AS zhangdiezhouqishu,T4.code,T4.riqi AS kaishiriqi,T3.riqi,
                       T3.riqihao,T3.shitifudu,T3.zhangdie,T4.riqihao AS zuigaojiariqihao,T3.riqihao AS zuihouoqihao,
                       T3.di,T3.kai,T3.shou,T3.gao,MIN(T3.shitifudu) OVER (PARTITION BY T3.code) AS zuidadiefu
                FROM  T4 INNER JOIN T3 ON T4.code = T3.code AND T4.riqihao < T3.riqihao
                WHERE    T4.RowID = 1)
			,T6  AS (
				SELECT * FROM T)
			,T10 AS (
				--查找后续阴线 并且是上一个交易日期
                SELECT   *
                FROM     T5
                WHERE   T5.zuihouoqihao+1= (T5.zuigaojiariqihao+zhangdiezhouqishu))
			,T13 AS ( 
				SELECT T10.kaishiriqi AS gaoriqi,T10.code,T10.riqi AS ciriqi,T6.riqi AS zhuriqi,T10.shitifudu,
				T10.riqihao as ciriqihao,T10.zhangdie,T10.di
                FROM T10 INNER JOIN T6 ON T10.code = T6.code
                WHERE    T10.zuihouoqihao + 1 = t6.riqihao AND T10.zhangdie < 0	AND zuidadiefu=T10.shitifudu AND ABS(T10.shitifudu)/(100/61.8) < T6.shitifudu
                        AND T10.di * 1.01 >= T10.shou AND T6.di * 1.01 >= T6.kai)					
			,T15 AS ( 
				SELECT T13.ciriqihao,T13.code,T13.ciriqi
			    FROM T13)		 
			,T11 AS (
				SELECT ROW_NUMBER() OVER (PARTITION BY T3.code ORDER BY di ASC) AS RowID2,T3.*
				FROM T3 INNER JOIN T15 ON T15.code = T3.code
                WHERE T15.ciriqihao-15 < T3.riqihao  AND T3.riqihao < T15.ciriqihao)		 
			,T12 AS ( 
					SELECT   *
                    FROM     T11
                    WHERE RowID2 = 1)
					 

           
----第二天阳线收复
--INSERT INTO [dbo].[T900]
--( [gaoriqi] ,
--[code] ,
--[ciriqi] ,
--[zhuriqi] ,
--[shitifudu] ,
--[yingxianshu] ,
--[yangxianshu] ,
--[riqihao] ,
--[di] ,
--[riqi]
--)
--                SELECT DISTINCT T13.*,T12.riqi
				INTO [dbo].[T900]
				FROM T13 INNER JOIN T12 ON T12.code = T13.code
                WHERE T12.di > T13.di AND (T12.riqihao+13 = T13.ciriqihao OR T12.riqihao+5 = T13.ciriqihao OR T12.riqihao+8 = T13.ciriqihao)
		 
       SET @i = @i - 1;	
	END
