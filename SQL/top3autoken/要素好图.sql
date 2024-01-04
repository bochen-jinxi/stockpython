--4要素好图
--买点描述： 好图
 -----------------------------------------------------------------------------------
    
USE stock 
go 

--SELECT    ROW_NUMBER() OVER( PARTITION BY code ORDER BY riqi Desc) AS riqihao,*
--INTO T90
--FROM     dbo.lishijiager
--WHERE code='sz.002920' and  riqi >='2022-12-21' and riqi <='2023-01-20'

;WITH T AS (
		SELECT riqihao,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],
		[kai],[shou],[di],[gao],[chengjiaoliang],[pctChg],
		IIF(shou>=kai,shou,kai)as maxval,IIF(shou<=kai,shou,kai) AS minval				
		FROM     dbo.T90
		WHERE    riqihao <= 8)
		
		--SELECT * FROM T	
		
	,T3 AS ( 	 
		SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY shitifudu DESC) AS RowID,*,(gao/maxVal-1)*100 AS shangyingxianfudu,(minval/di-1) *100 AS xiayingxianfudu
		FROM T)
		
		--SELECT * FROM T3
		
	,T4 AS ( 
		-- 各代码最大实体的日期 价格
		SELECT   *
		FROM     T3
		WHERE    RowID = 1 AND riqihao>=8-3  )

		--SELECT * FROM T4
		
	,T5 AS (
		--见最大实体后 后续价格数据中所有阴阳线 并统计后续阴阳线的数量
		SELECT COUNT(1) OVER (PARTITION BY T3.code) AS zhangdiezhouqishu,T3.[pctChg],
		T4.code,T4.riqi AS kaishiriqi,T3.riqi,
			   T3.riqihao,T3.shitifudu,T4.riqihao AS zuigaojiariqihao,T3.riqihao AS zuihouriqihao,
			   T3.di,T3.kai,T3.shou,T3.gao,MIN(T3.[pctChg]) OVER (PARTITION BY T3.code) AS zuidadiehuozezuixiaozhang,
			   Max(T3.shou) OVER (PARTITION BY T3.code) AS zuidashou,
			   T3.shangyingxianfudu,T3.xiayingxianfudu
		FROM  T4 INNER JOIN T3 ON T4.code = T3.code AND T4.riqihao > T3.riqihao
		WHERE    T4.RowID = 1)

		--SELECT * FROM T5

		 ,T590 AS (
		SELECT COUNT(1) OVER (PARTITION BY T5.code) AS suoyoumanzu ,* FROM T5  
		 WHERE shangyingxianfudu<1.15 AND xiayingxianfudu<1.15
		 )
		 
		 --SELECT * FROM T590
		 
		,T501 AS (
		SELECT (CASE WHEN [pctChg]>=0 THEN 1 ELSE 0 END) AS zhangdie, code,kaishiriqi 
		FROM T5)
		
		,T502 AS (
		SELECT  COUNT(1) OVER (PARTITION BY T501.code) AS yangxianshu, code,kaishiriqi 
		FROM T501 
		WHERE zhangdie=1)
		
		,T503 AS (
		SELECT  COUNT(1) OVER (PARTITION BY T501.code) AS yinxianshu, code,kaishiriqi 
		FROM T501
		WHERE zhangdie=0)
		
		,T504 AS (
		SELECT yangxianshu, yinxianshu,  ISNULL(T502.code,T503.code) AS  code,ISNULL(T502.kaishiriqi,T503.kaishiriqi) AS kaishiriqi
		FROM T502 FULL JOIN T503  ON T502.code = T503.code AND T502.kaishiriqi = T503.kaishiriqi )
		
		--SELECT * FROM T504
				
		,T508 AS (
		SELECT  COUNT(1) OVER (PARTITION BY T5.code) AS wushangyingxianfudushu,code,kaishiriqi 
		FROM T5 
		WHERE  shangyingxianfudu=0)
		
		--SELECT * FROM T508
		
		,T509 AS (
		SELECT COUNT(1) OVER (PARTITION BY T5.code) AS  wuxiayingxianfudushu, code,kaishiriqi 
		FROM T5 
		WHERE  xiayingxianfudu=0)   
		
		--SELECT * FROM T509

		,T510 AS (	
		SELECT  wushangyingxianfudushu , wuxiayingxianfudushu, ISNULL(T508.code,T509.code) AS code,ISNULL(T508.kaishiriqi,T509.kaishiriqi) AS kaishiriqi  
		FROM T508 FULL JOIN T509 ON T508.code = T509.code AND T508.kaishiriqi = T509.kaishiriqi)
		
		--SELECT * FROM T510
		
		,T599 AS 
		(				
		SELECT T590.*,yangxianshu,yinxianshu,wushangyingxianfudushu,wuxiayingxianfudushu 
		FROM T590 LEFT JOIN T504 ON T590.code = T504.code  AND T590.kaishiriqi = T504.kaishiriqi  
		 LEFT JOIN T510 ON T590.code = T510.code  AND T590.kaishiriqi = T510.kaishiriqi)
		 
		--SELECT * FROM T599	
			
		,T6 AS (
		SELECT *
		FROM T599)
		
		-- SELECT * FROM T6 				--where code ='sz.300402'
		
		INSERT INTO dbo.T10000( zuidadiehuozezuixiaozhang, zuidashou,suoyoumanzu,  zhangdiezhouqishu,kaishiriqi,yangxianshu,yinxianshu,wushangyingxianfudushu,wuxiayingxianfudushu,code)
		SELECT  DISTINCT zuidadiehuozezuixiaozhang , zuidashou, suoyoumanzu,  zhangdiezhouqishu,kaishiriqi,ISNULL(yangxianshu,0) AS yangxianshu,ISNULL(yinxianshu,0) AS yinxianshu,ISNULL(wushangyingxianfudushu,0) AS wushangyingxianfudushu,ISNULL(wuxiayingxianfudushu,0) AS wuxiayingxianfudushu,code
		--INTO  T10000
		 FROM T6  
		--WHERE  suoyoumanzu=zhangdiezhouqishu 
		  --AND yangxianshu>yinxianshu
		 

	
	 
