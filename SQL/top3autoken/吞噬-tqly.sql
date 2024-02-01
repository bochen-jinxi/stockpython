--4����
--�������������
 -----------------------------------------------------------------------------------
    
USE stock 
go 
--DROP TABLE T10002
go
DROP TABLE T90 
go
SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi Desc) AS riqihao,*
INTO T90
FROM lishijiager
--�����ҵ
WHERE riqi>='2020-11-01' AND riqi<='2020-11-20' AND code='sz.002466' 
--WHERE riqi>='2023-12-01' AND riqi<='2024-01-30'  
--SELECT * FROM T90

;WITH T AS (
	SELECT riqihao,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],
	[kai],[shou],[di],[gao],[chengjiaoliang],[pctChg],
	IIF(shou>=kai,shou,kai)as maxval,IIF(shou<=kai,shou,kai) AS minval				
	FROM T90
	WHERE riqihao<=13)
	--SELECT * FROM T		
,T3 AS ( 	 
	SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY shitifudu DESC) AS RowID,*,(gao/maxVal-1)*100 AS shangyingxianfudu,(minval/di-1) *100 AS xiayingxianfudu
	FROM T)
	--SELECT * FROM T3		
,T4 AS ( 
	-- ���������ʵ������� �۸�
	SELECT *
	FROM T3
	WHERE riqihao=1 )
	--SELECT * FROM T4		
,T499 AS (
	--�����ʵ��� �����۸����������������� ��ͳ�ƺ��������ߵ�����
	SELECT COUNT(1) OVER (PARTITION BY T3.code) AS zhangdiezhouqishu,T3.[pctChg],T4.di AS kaishidi,T4.gao AS kaishigao,
	T4.code,T4.riqi AS kaishiriqi,T3.riqi,MAX(T3.riqi) OVER (PARTITION BY T3.code) AS zhuyiriqi,
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
	FROM T4 INNER JOIN T3 ON T4.code = T3.code AND T4.riqihao < T3.riqihao
	)
	--SELECT * FROM T499
,T6	AS ( 
	-- �������ݰ�����������   
	SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS riqihaoasc,*
	FROM T499)
	--SELECT * FROM T6
,T7 AS (
	--���Һ������������߲����°����������� ���Բ����������ںŵ�����
	SELECT riqihaoasc-ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS lianxuxiadieriqizu,*
	FROM T6
	WHERE pctChg>0)
	--SELECT * FROM T7
,T8 AS (
	--��ʶ�����������������ߵ�����
	SELECT COUNT(1) OVER (PARTITION BY code, lianxuxiadieriqizu) AS lianxuxiadieshu,*
	FROM T7)
	--SELECT * FROM T8
,T9 AS ( 		  
	--��ʶ��������������������� 
	SELECT MAX(lianxuxiadieshu) OVER (PARTITION BY code) AS zuidalianxushangzhangshu,*
	FROM T8)
	--SELECT * FROM T9
,T10 AS ( 
	SELECT *
	FROM T9 
	WHERE zuidalianxushangzhangshu>= 3
	)
	--SELECT * FROM T10
,T5 AS (
	SELECT *
	, kaishigao/1.15 AS a,zuidagao AS b , kaishidi/1.15 AS c ,zuixiaodi AS d
 
	FROM  T10  
	WHERE kaishigao/1.15<zuidagao AND kaishidi/1.15<zuixiaodi
	AND  zuidashangyingxianfudu<8 AND zuidaxiayingxianfudu<6
	)	
	--SELECT * FROM T5	 		
,T590 AS (
	SELECT COUNT(1) OVER (PARTITION BY T5.code) AS suoyoumanzu ,
	MIN(T5.riqi) OVER(PARTITION BY code) AS jieshuriqi ,
	* 
	FROM T5  
	--�κ�һ����������Ӱ�߲���0.5
	WHERE (shangyingxianfudu<=4 AND xiayingxianfudu<=1)
	--�κ�һ�������ͷ�����κ�һ��������
	OR (shangyingxianfudu=0 OR  xiayingxianfudu=0))	 
	--SELECT * FROM T590 
,T501 AS (
	SELECT code,kaishiriqi,zhuyiriqi,jieshuriqi	
	FROM T590)
	--SELECT * FROM T501		
,T599 AS 
(				
	SELECT T590.*
	FROM T590 LEFT JOIN T4 AS A ON T590.code = A.code  AND T590.kaishiriqi = A.riqi
	LEFT JOIN T499 AS B ON T590.code = B.code  AND T590.zhuyiriqi = B.riqi
	--LEFT JOIN T499 AS C ON T590.code = C.code  AND T590.jieshuriqi = C.riqi
	WHERE --C.di/1.009<A.kai 
	EXISTS ( SELECT 1 FROM T499 AS C WHERE T590.code = C.code AND T590.jieshuriqi = C.riqi AND C.di/1.009 < A.kai)
	AND  (A.kai<B.shou AND A.shou>B.kai AND B.pctChg<0 ) OR (A.kai<B.kai AND A.shou>B.shou AND B.pctChg>0 )  )	 
	--SELECT * FROM T599				
,T600 AS (
	SELECT T599.*
	FROM T599 
	INNER JOIN T4 ON T599.code = T4.code 
	WHERE T599.kaishiriqi=T4.riqi
	AND T4.di>T4.kai/1.009
	)		
	--SELECT * FROM T600 
	
	--SELECT DISTINCT zuidalianxushangzhangshu,zuidadiehuozezuixiaozhang,zuidashou,suoyoumanzu,zhangdiezhouqishu,kaishiriqi,jieshuriqi,ISNULL(yangxianshu,0) AS yangxianshu,ISNULL(yinxianshu,0) AS yinxianshu,ISNULL(wushangyingxianfudushu,0) AS wushangyingxianfudushu,ISNULL(wuxiayingxianfudushu,0) AS wuxiayingxianfudushu,code
	INSERT INTO T10002([kaishiriqi],[jieshuriqi]   ,[code])
	SELECT DISTINCT  kaishiriqi,jieshuriqi,code		
	--INTO T10002
	FROM T600  
	--WHERE yangxianshu>=yinxianshu
	--ORDER BY zuidashou desc
	
	 
