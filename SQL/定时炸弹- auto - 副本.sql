--SCS买点5：定时炸弹
--买点描述：低位十字孕线+不创新低的实体阳线。 在下跌波段中，第一天收出创新低大阴性，第二天高开，经过一天震荡收出十字星孕线。第三天收出不创新低的大实体阳线
 -----------------------------------------------------------------------------------
  
use stock 
go 
--SELECT ROW_NUMBER() OVER( PARTITION BY code ORDER BY riqi ASC) AS riqihao,*
--INTO T90
--FROM     dbo.lishijiager
--WHERE  riqi >='2022-12-01' and riqi <='2023-01-12'

DECLARE @i INT ;
SET @i=(SELECT COUNT(1) FROM dbo.T90 WHERE  code ='sz.000001')
WHILE(@i>5)
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
					 SELECT A.gaoriqi, A.riqi AS mairiqi,B.riqi AS zhariqi,c.riqi AS zhuriqi,A.code,C.riqihao FROM T12 AS A INNER JOIN T90 AS B ON A.code = B.code INNER JOIN dbo.T90 AS C ON B.code = C.code
					 WHERE A.riqihao+1=B.riqihao AND B.riqihao+1=C.riqihao AND A.shou<A.di*1.01 AND  B.kai>A.shou  AND B.kai*1.01>B.shou
					 AND B.di*1.01>B.kai AND C.di>A.di AND C.di>B.di AND C.pctChg>0 AND C.shou>c.gao/1.01 AND c.di*1.01>c.kai
					 )
					 INSERT INTO dbo.T904 ([gaoriqi],[mairiqi],[zhariqi],[zhuriqi],[code])
					 SELECT [gaoriqi],[mairiqi],[zhariqi],[zhuriqi],[code]  FROM T13 WHERE riqihao=@i
			 
		SET @i=@i-1;
	END