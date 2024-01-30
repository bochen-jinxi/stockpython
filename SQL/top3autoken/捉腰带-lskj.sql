--4捉腰带
--买点描述： 捉腰带
 -----------------------------------------------------------------------------------
    
USE stock 
go 
--DROP TABLE T10001
go
DROP TABLE T90 
go
SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi Desc) AS riqihao,*
INTO T90
FROM lishijiager
--蓝思科技
WHERE riqi>='2019-08-14' AND riqi<='2019-08-26' AND code='sz.300433' 
--WHERE riqi>='2023-12-01' AND riqi<='2024-01-26'  
--SELECT * FROM T90

;WITH T AS (
	SELECT riqihao,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],
	[kai],[shou],[di],[gao],[chengjiaoliang],[pctChg],
	IIF(shou>=kai,shou,kai)as maxval,IIF(shou<=kai,shou,kai) AS minval				
	FROM T90
	WHERE riqihao<=9)
	--SELECT * FROM T		
,T3 AS ( 	 
	SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY shitifudu DESC) AS RowID,*,(gao/maxVal-1)*100 AS shangyingxianfudu,(minval/di-1) *100 AS xiayingxianfudu
	FROM T)
	--SELECT * FROM T3		
,T4 AS ( 
	-- 各代码最大实体的日期 价格
	SELECT *
	FROM T3
	WHERE RowID=1 AND riqihao>=8-3)
	--SELECT * FROM T4		
,T499 AS (
	--见最大实体后 后续价格数据中所有阴阳线 并统计后续阴阳线的数量
	SELECT COUNT(1) OVER (PARTITION BY T3.code) AS zhangdiezhouqishu,T3.[pctChg],T4.di AS kaishidi,T4.gao AS kaishigao,
	T4.code,T4.riqi AS kaishiriqi,T3.riqi,MAX(T3.riqi) OVER (PARTITION BY T3.code) AS jieshuriqi,
	T3.riqihao,T3.shitifudu,T4.riqihao AS zuigaojiariqihao,T3.riqihao AS zuihouriqihao,
	T3.di,T3.kai,T3.shou,T3.gao,
	COUNT(CASE WHEN T3.shangyingxianfudu = 0 THEN 1 END) OVER(PARTITION BY T3.code) AS wushangyingxianfudushu,
	COUNT(CASE WHEN T3.xiayingxianfudu = 0 THEN 1 END) OVER(PARTITION BY T3.code) AS wuxiayingxianfudushu,
	COUNT(CASE WHEN T3.pctChg >= 0 THEN 1 END) OVER(PARTITION BY T3.code) AS yangxianshu,
	COUNT(CASE WHEN T3.pctChg < 0 THEN 1 END) OVER(PARTITION BY T3.code) AS yinxianshu,
	MIN(T3.[pctChg]) OVER (PARTITION BY T3.code) AS zuidadiehuozezuixiaozhang,
	Max(T3.gao) OVER (PARTITION BY T3.code) AS zuidagao,
	Min(T3.di) OVER (PARTITION BY T3.code) AS zuixiaodi,
	Max(T3.shou) OVER (PARTITION BY T3.code) AS zuidashou,
	Max(T3.shangyingxianfudu) OVER (PARTITION BY T3.code) AS zuidashangyingxianfudu,
	Max(T3.xiayingxianfudu) OVER (PARTITION BY T3.code) AS zuidaxiayingxianfudu,
	T3.shangyingxianfudu,T3.xiayingxianfudu
	FROM T4 INNER JOIN T3 ON T4.code = T3.code AND T4.riqihao > T3.riqihao
	WHERE T4.RowID=1)
	--SELECT * FROM T499
,T6	AS ( 
	-- 后续数据按日期正序标号   
	SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS riqihaoasc,*
	FROM T499)
	--SELECT * FROM T6
,T7 AS (
	--查找后续中所有阳线并重新按日期正序标号 用以查找连续日期号的阳线
	SELECT riqihaoasc-ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS lianxuxiadieriqizu,*
	FROM T6
	WHERE pctChg>0)
	--SELECT * FROM T7
,T8 AS (
	--标识后续中所有连续阳线的天数
	SELECT COUNT(1) OVER (PARTITION BY code, lianxuxiadieriqizu) AS lianxuxiadieshu,*
	FROM T7)
	-- SELECT * FROM T8
,T9 AS ( 		  
	--标识后续中阳线最大连续天数 
	SELECT MAX(lianxuxiadieshu) OVER (PARTITION BY code) AS zuidalianxushangzhangshu,*
	FROM T8)
	--SELECT * FROM T9
,T10 AS ( 
	SELECT *
	FROM T9 
	WHERE zuidalianxushangzhangshu>= 2
	)
,T5 AS (
	SELECT *
	FROM  T10  
	WHERE kaishigao*1.15>zuidagao AND kaishidi/1.04<zuixiaodi
	AND  zuidashangyingxianfudu<6 AND zuidaxiayingxianfudu<4)	
	--SELECT * FROM T5	 		
,T590 AS (
	SELECT COUNT(1) OVER (PARTITION BY T5.code) AS suoyoumanzu ,* 
	FROM T5  
	--任何一天满足上下影线不过0.5
	WHERE (shangyingxianfudu<=4 AND xiayingxianfudu<=1)
	--任何一天满足光头或者任何一天满足光脚
	OR (shangyingxianfudu=0 OR  xiayingxianfudu=0))	 
	--SELECT * FROM T590 
,T501 AS (
	SELECT code,kaishiriqi	
	FROM T5)
	--SELECT * FROM T501		
,T599 AS 
(				
	SELECT T590.* 
	FROM T590 LEFT JOIN T501 ON T590.code = T501.code  AND T590.kaishiriqi = T501.kaishiriqi)	 
	--SELECT * FROM T599				
,T600 AS (
	SELECT T599.*
	FROM T599 
	FULL JOIN T6 ON T599.code = T6.code 
	WHERE T599.jieshuriqi=T6.riqi
	AND T6.di>T6.kai/1.009
	)		
	--SELECT * FROM T600 
	
	--SELECT DISTINCT zuidalianxushangzhangshu,zuidadiehuozezuixiaozhang,zuidashou,suoyoumanzu,zhangdiezhouqishu,kaishiriqi,jieshuriqi,ISNULL(yangxianshu,0) AS yangxianshu,ISNULL(yinxianshu,0) AS yinxianshu,ISNULL(wushangyingxianfudushu,0) AS wushangyingxianfudushu,ISNULL(wuxiayingxianfudushu,0) AS wuxiayingxianfudushu,code
	INSERT INTO T10000([kaishiriqi],[jieshuriqi]   ,[code])
	SELECT DISTINCT  kaishiriqi,jieshuriqi,code		
	--INTO T10001
	FROM T600  
	--WHERE yangxianshu>=yinxianshu
	--ORDER BY zuidashou desc
	
	 
