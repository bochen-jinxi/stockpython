--4要素好图
--买点描述： 好图
 -----------------------------------------------------------------------------------
    
USE stock 
go 



--DROP TABLE T90 
--go
--SELECT    ROW_NUMBER() OVER( PARTITION BY code ORDER BY riqi Desc) AS riqihao,*
--INTO T90
--FROM     dbo.lishijiager
--WHERE   riqi >='2022-06-27' and riqi <='2024-01-04'



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
		WHERE    RowID = 1 AND kai=di  AND riqihao>8-3  )

		--SELECT * FROM T4
		
	,T499 AS (
		--见最大实体后 后续价格数据中所有阴阳线 并统计后续阴阳线的数量
		SELECT COUNT(1) OVER (PARTITION BY T3.code) AS zhangdiezhouqishu,T3.[pctChg],T4.di AS kaishidi,
		T4.code,T4.riqi AS kaishiriqi,T3.riqi,MAX(T3.riqi) OVER (PARTITION BY T3.code) AS jieshuriqi,
			   T3.riqihao,T3.shitifudu,T4.riqihao AS zuigaojiariqihao,T3.riqihao AS zuihouriqihao,
			   T3.di,T3.kai,T3.shou,T3.gao,MIN(T3.[pctChg]) OVER (PARTITION BY T3.code) AS zuidadiehuozezuixiaozhang,
			   Max(T3.shou) OVER (PARTITION BY T3.code) AS zuidashou,
			   MIN(T3.shou) OVER (PARTITION BY T3.code) AS jieshushou,
			   T3.shangyingxianfudu,T3.xiayingxianfudu
		FROM  T4 INNER JOIN T3 ON T4.code = T3.code AND T4.riqihao > T3.riqihao
		WHERE    T4.RowID = 1)

		--SELECT * FROM T499

		,T5 AS (
		SELECT *
		FROM  T499 
		WHERE kaishidi<jieshushou )
		
		--SELECT * FROM T5	 
			
		 ,T590 AS (
		SELECT COUNT(1) OVER (PARTITION BY T5.code) AS suoyoumanzu ,* FROM T5  
		--任何一天满足上下影线不过0.5
		 WHERE (shangyingxianfudu<=0.5 AND xiayingxianfudu<=0.5)
		--任何一天满足光头或者任何一天满足光脚
		 OR (shangyingxianfudu=0 OR  xiayingxianfudu=0)
		 )
		 
		 --SELECT * FROM T590
		 
		,T501 AS (
		SELECT code,kaishiriqi,
		   COUNT(CASE WHEN T5.pctChg >= 0 THEN 1 END) OVER(PARTITION BY T5.code) AS yangxianshu,
           COUNT(CASE WHEN T5.pctChg < 0 THEN 1 END) OVER(PARTITION BY T5.code) AS yinxianshu,
           COUNT(CASE WHEN T5.shangyingxianfudu = 0 THEN 1 END) OVER(PARTITION BY T5.code) AS wushangyingxianfudushu,
           COUNT(CASE WHEN T5.xiayingxianfudu = 0 THEN 1 END) OVER(PARTITION BY T5.code) AS wuxiayingxianfudushu
		FROM T5)
		 
		
		--SELECT * FROM T501
		
		,T599 AS 
		(				
		SELECT T590.*,yangxianshu,yinxianshu,wushangyingxianfudushu,wuxiayingxianfudushu 
		FROM T590 
		 LEFT JOIN T501 ON T590.code = T501.code  AND T590.kaishiriqi = T501.kaishiriqi)
		 
		--SELECT * FROM T599	
			
		,T6 AS (
		SELECT *
		FROM T599)
		
		-- SELECT * FROM T6 				--where code ='sz.300402'
		
		INSERT INTO dbo.T10000( zuidadiehuozezuixiaozhang, zuidashou,suoyoumanzu,  zhangdiezhouqishu,kaishiriqi,jieshuriqi,yangxianshu,yinxianshu,wushangyingxianfudushu,wuxiayingxianfudushu,code)
		SELECT  DISTINCT zuidadiehuozezuixiaozhang , zuidashou, suoyoumanzu,  zhangdiezhouqishu,kaishiriqi,jieshuriqi,ISNULL(yangxianshu,0) AS yangxianshu,ISNULL(yinxianshu,0) AS yinxianshu,ISNULL(wushangyingxianfudushu,0) AS wushangyingxianfudushu,ISNULL(wuxiayingxianfudushu,0) AS wuxiayingxianfudushu,code
		--INTO  T10000
		 FROM T6  
		--WHERE  suoyoumanzu=zhangdiezhouqishu 
		  --AND yangxianshu>yinxianshu
		 

	
	 
