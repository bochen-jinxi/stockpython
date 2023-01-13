----SCS买点2：绝地反击
---买点描述：股价在连续下跌后收出低位大阴线形态，第二天股价低开后高走，盘间最高价回补缺口
---最终低开收阳的倒锤子形态
 
 --USE stock 
--go 

--SELECT    ROW_NUMBER() OVER( PARTITION BY code ORDER BY riqi ASC) AS riqihao,*
--INTO T90
--FROM     dbo.lishijiager
--WHERE  riqi >='2022-12-01' and riqi <='2023-01-12'

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
				-- 后续数据按日期正序标号  最低价倒序 zuidariqihao 只最大日期最大  zuidijiahao 值最大价格最低
				SELECT COUNT(1) OVER ( PARTITION BY T5.code ) AS zhangdiezhouqishu,ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS  zuidariqihao, ROW_NUMBER() OVER (PARTITION BY code ORDER BY di DESC) AS zuidijiahao,*
				FROM T5)
			--SELECT * FROM T6
			 ,T7 AS (
				 --查找后续中所有阴线并重新按日期正序标号 用以查找连续日期号的阴线  lianxuxiadieriqizu  
				SELECT   zuidariqihao - ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS lianxuxiadieriqizu,ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS xiadiexuhao,COUNT(1) OVER(PARTITION BY code) AS  yingxianshu,(SELECT COUNT(1) FROM T6 AS A WHERE A.zhangdie>0 AND A.code=T6.code ) AS yangxianshu,*
				FROM  T6
				WHERE  zhangdie < 0)
			-- SELECT * FROM T7
			 ,T8 AS (
				--标识后续中所有连续阴线的天数
				SELECT COUNT(1) OVER (PARTITION BY code, lianxuxiadieriqizu) AS lianxuxiadieshu,*
				FROM T7)
			-- SELECT * FROM T8
			,T9 AS ( 
				--标识后续中阴线最大连续天数 
				SELECT MAX(lianxuxiadieshu) OVER (PARTITION BY code) zuidalianxuxiadieshu,*
				FROM T8)
			 --SELECT * FROM T9
			 ,T10 AS (
				--查找后续连续下跌日期
				SELECT * FROM T9 WHERE zuidalianxuxiadieshu=lianxuxiadieshu)
			 ,T11 AS (
				SELECT ROW_NUMBER() OVER( PARTITION BY  code ORDER BY riqi DESC ) AS zuidaqiri, * FROM T10)
			 ,T12 AS (
				SELECT DISTINCT T11.*,T90.riqihao 
				FROM T11 INNER JOIN T90 ON T11.code = T90.code  
				WHERE zuidaqiri = 1 AND T11.riqi = T90.riqi 	
			 )

			INSERT INTO T901(
			[gaoriqi]	,
			[lianxuriqi],
			[zhuyiriqi]	,
			[code]	
			)
			SELECT T12.gaoriqi,  T12.riqi AS  lianxuriqi,T3.riqi AS zhuyiriqi,T12.code   
			FROM T12 INNER JOIN T3 ON T12.code = T3.code 
			WHERE T12.riqihao+1=T3.riqihao  AND  T3.riqihao=@i AND T12.di*1.01 >= T12.shou AND T12.di > T3.kai*1.01	AND T3.gao > T12.shou AND T3.kai < T3.di*1.01
		 
       SET @i = @i - 1;	
	END

		 		
		  
