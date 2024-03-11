--4要素好图
--买点描述： 好图
 -----------------------------------------------------------------------------------
    
USE stock 
go 
--DROP TABLE T10000
go
DROP TABLE T90 
go
SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi Desc) AS riqihao,*
INTO T90
FROM lishijiager
--新天绿能
--WHERE   riqi >='2021-02-10' and riqi <='2021-03-11' AND code='sh.600956'
WHERE riqi>='2023-12-01' AND riqi<='2024-03-08'  
--SELECT * FROM T90

;WITH T AS (
	SELECT riqihao,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],
	[kai],[shou],[di],[gao],[chengjiaoliang],[pctChg],
	IIF(shou>=kai,shou,kai)as maxval,IIF(shou<=kai,shou,kai) AS minval				
	FROM T90
	WHERE riqihao<=17)
	--SELECT * FROM T		
,T3 AS ( 	 
	SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY shitifudu DESC) AS RowID,*,(gao/maxVal-1)*100 AS shangyingxianfudu,(minval/di-1) *100 AS xiayingxianfudu
	FROM T)
    --SELECT * FROM T3	
,T401 AS ( 
	--找跳空
	SELECT T.*,
	CASE
        WHEN T.kai>A.shou THEN T.kai/A.shou-1 -- 当前值大于前值
        WHEN T.kai<A.shou THEN 1-A.shou/T.kai -- 当前值小于前值
        ELSE 0 -- 当前值等于前值
        END AS val  
	FROM T INNER JOIN T AS A ON T.code = A.code 
	WHERE T.riqihao+1=A.riqihao)
	--SELECT * FROM T401
,T402 AS ( 
	SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY val DESC) AS RowID,* 
	FROM T401 
	WHERE val>0)
,T4 AS ( 
	SELECT * 
	FROM T402 
	WHERE RowID=1  AND riqihao>=16-3 AND val>0.05)
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
	-- SELECT * FROM T9
,T10 AS ( 
	SELECT TOP 1 *
	FROM T9 
	WHERE zuidalianxushangzhangshu>=6)
	--SELECT * FROM T10
,T5 AS (
	SELECT T499.*,(SELECT zuidalianxushangzhangshu FROM T10 WHERE T10.code=T499.code) AS zuidalianxushangzhangshu
	FROM  T499 
	WHERE zuidagao/kaishigao-1<0.07  AND ABS(1-kaishidi/zuixiaodi)<0.05
	AND  zuidashangyingxianfudu<4 AND zuidaxiayingxianfudu<4)		
	--SELECT * FROM T5	 		
,T590 AS (
	SELECT COUNT(1) OVER (PARTITION BY T5.code) AS suoyoumanzu ,* FROM T5  
	--任何一天满足上下影线不过0.5
	WHERE (shangyingxianfudu<=0.5 AND xiayingxianfudu<=0.6)
	--任何一天满足光头或者任何一天满足光脚
	OR (shangyingxianfudu=0 OR  xiayingxianfudu=0))	 
	--SELECT * FROM T590 

,T501 AS (
	SELECT DISTINCT code,kaishiriqi,jieshuriqi,yangxianshu,yinxianshu	
	FROM T590
	WHERE yangxianshu>yinxianshu)
	--SELECT * FROM T501	
,T502 AS (
	SELECT T3.*,kaishiriqi 	
	FROM  T3 LEFT JOIN T501 ON T3.code = T501.code  and  T3.riqi = T501.kaishiriqi 
	WHERE  T501.kaishiriqi IS NOT NULL)
,T503 AS (
	SELECT T3.*,jieshuriqi 
	FROM  T3 LEFT JOIN T501 ON T3.code = T501.code  and  T3.riqi = T501.jieshuriqi 
	WHERE  T501.jieshuriqi IS NOT NULL)		
,T599 AS (				
	SELECT A.kaishiriqi,B.jieshuriqi,A.code
	FROM T502 AS A INNER JOIN T503 AS B	ON A.code=B.code
	WHERE B.pctChg>0 AND ABS(1-A.maxval/B.maxval)<0.02 AND B.maxval>A.maxval)	
	--SELECT * FROM T599 
	--SELECT DISTINCT zuidalianxushangzhangshu,zuidadiehuozezuixiaozhang,zuidashou,suoyoumanzu,zhangdiezhouqishu,kaishiriqi,jieshuriqi,ISNULL(yangxianshu,0) AS yangxianshu,ISNULL(yinxianshu,0) AS yinxianshu,ISNULL(wushangyingxianfudushu,0) AS wushangyingxianfudushu,ISNULL(wuxiayingxianfudushu,0) AS wuxiayingxianfudushu,code
	INSERT INTO T10000([kaishiriqi],[jieshuriqi]   ,[code])
	SELECT DISTINCT  kaishiriqi,jieshuriqi,code		
	--INTO T10000
	FROM T599  
	--ORDER BY zuidashou desc
	
	 
