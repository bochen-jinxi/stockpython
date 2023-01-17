----SCS买点2：绝地反击
---买点描述：股价在连续下跌后收出低位大阴线形态，第二天股价低开后高走，盘间最高价回补缺口
---最终低开收阳的倒锤子形态
 
 --USE stock 
--go 

--SELECT    ROW_NUMBER() OVER( PARTITION BY code ORDER BY riqi ASC) AS riqihao,*
--INTO T90
--FROM     dbo.lishijiager
--WHERE  riqi >='2022-08-01' and riqi <='2023-01-16'

DECLARE @i INT;
SET @i = (SELECT COUNT(1) FROM  dbo.T90 WHERE code = 'sz.000001')
WHILE (@i > 5) 
	BEGIN
		;WITH T AS (
						SELECT riqihao,pctChg AS zhangdie,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],[kai],[shou],[di],[gao],[chengjiaoliang],[pctChg] 
						FROM     dbo.T90
						WHERE riqihao <= @i)
						--SELECT * FROM T	
					,T3 AS ( 	 
						SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY gao DESC) AS RowID,*
						FROM T)
					,T4 AS ( 
						-- 各代码见高点的日期 价格
						SELECT   *
						FROM     T3
						WHERE    RowID = 1 )
						--SELECT * FROM T4
					,T5 AS (
		  				--见高点后 后续价格数据中所有阴阳线 并统计后续阴阳线的数量
						SELECT DISTINCT T4.code,T4.riqi AS gaoriqi,T3.riqi,T3.shitifudu,T3.zhangdie,T3.di,T3.kai,T3.shou,T3.gao,MIN(T3.[pctChg]) OVER(PARTITION BY T3.code) AS zuidadiefu
						FROM T4 INNER JOIN T3 ON T4.code = T3.code  AND T4.riqi < T3.riqi
						WHERE    T4.RowID = 1)
						--SELECT * FROM T5
					,T6 AS ( 
						-- 后续数据 zuidijiahao 值最小价格最低
						SELECT  ROW_NUMBER() OVER (PARTITION BY code ORDER BY di ASC) AS zuidijia, *
						FROM T5)
						--SELECT * FROM T6
					 ,T7 AS (
						 --查找后续中所有阴线 最低价日期
						SELECT  *
						FROM  T6
						WHERE  zhangdie < 0 AND zuidijia=1)
						--SELECT * FROM T7
					 ,T12 AS (
						SELECT DISTINCT T7.*,T90.riqihao 
						FROM T7 INNER JOIN T90 ON T7.code = T90.code  
						WHERE zuidijia = 1 AND T7.riqi = T90.riqi 	
					 )
					 --SELECT * FROM T12
					 ,T13 AS (
					 SELECT A.gaoriqi, B.riqi AS zhuyiriqi,A.code,B.shou,B.riqihao,B.kai,A.di FROM T12 AS A INNER JOIN T90 AS B ON A.code = B.code 
					 WHERE A.riqihao+1=B.riqihao 
					  AND A.shou<A.di*1.01 
					 AND  B.kai*1.005<A.shou  
					 AND B.kai<B.di*1.01
					 AND B.gao>A.shou
					 AND B.pctChg<1
					 )
					 INSERT INTO dbo.T901 (gaoriqi,zhuyiriqi,code,shou)
					 SELECT gaoriqi, zhuyiriqi,code,shou        
					 FROM T13 
					 WHERE riqihao=@i
       SET @i = @i - 1;	
	END

		 		
		  
