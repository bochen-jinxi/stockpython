--4���� �����ҵ�б���������ȱ����һ��ǿ�ҽ����ź�
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
WHERE riqi>='2020-11-02' AND riqi<='2020-11-20' AND code='sz.002466' 
--WHERE riqi>='2024-01-01' AND riqi<='2024-02-21' 
--SELECT * FROM T90
;WITH T AS (
	SELECT riqihao,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],
	[kai],[shou],[di],[gao],[chengjiaoliang],[pctChg],
	IIF(shou>=kai,shou,kai)as maxval,IIF(shou<=kai,shou,kai) AS minval				
	FROM T90
	WHERE riqihao<=16)
,T3 AS ( 	 
	SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY shitifudu DESC) AS RowID,*,(gao/maxVal-1)*100 AS shangyingxianfudu,(minval/di-1) *100 AS xiayingxianfudu
	FROM T)
	--SELECT * FROM T3		
,T401 AS ( 
	--������
	SELECT T.*,
	CASE
        WHEN T.kai>A.shou THEN T.kai/A.shou-1 -- ��ǰֵ����ǰֵ
        WHEN T.kai<A.shou THEN 1-A.shou/T.kai -- ��ǰֵС��ǰֵ
        ELSE 0 -- ��ǰֵ����ǰֵ
        END AS val 
	FROM T INNER JOIN T AS A ON T.code = A.code 
	WHERE T.riqihao+1=A.riqihao)
	--SELECT * FROM T401
,T402 AS ( 
	SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqihao DESC) AS RowID,* 
	FROM T401 
	WHERE val>0)
	--SELECT * FROM T402
,T403 AS ( 
	SELECT * 
	FROM T402 
	WHERE RowID=1  AND riqihao>=15-2)
	--SELECT * FROM T403
,T4 AS ( 
	SELECT *
	FROM T403)
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
	FROM T4 INNER JOIN T3 ON T4.code = T3.code AND T4.riqihao > T3.riqihao
	WHERE T4.RowID=1)
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
	SELECT *	,1-kaishidi/zuixiaodi AS   dfgdfgdfgd
	FROM  T10  
	WHERE zuidagao/kaishigao-1<0.26 AND ABS(1-kaishidi/zuixiaodi)<0.01
	AND  zuidashangyingxianfudu<8 AND zuidaxiayingxianfudu<6)	
	--SELECT * FROM T5	 		
,T590 AS (
	SELECT COUNT(1) OVER (PARTITION BY T5.code) AS suoyoumanzu,MIN(T5.riqi) OVER(PARTITION BY code) AS jieshuriqi,* 
	FROM T5  
	--�κ�һ����������Ӱ�߲���0.5
	WHERE (shangyingxianfudu<=4 AND xiayingxianfudu<=1)
	--�κ�һ�������ͷ�����κ�һ��������
	OR (shangyingxianfudu=0 OR  xiayingxianfudu=0))	 
	--SELECT * FROM T590 

,T501 AS (
	SELECT DISTINCT code,kaishiriqi,jieshuriqi,yangxianshu,yinxianshu	
	FROM T590
	WHERE yangxianshu>yinxianshu)
	--SELECT * FROM T501	
,T502 AS (
	SELECT T401.*,kaishiriqi 	
	FROM  T401 LEFT JOIN T501 ON T401.code = T501.code  and  T401.riqi = T501.kaishiriqi 
	WHERE  T501.kaishiriqi IS NOT NULL)
,T503 AS (
	SELECT T401.*,jieshuriqi 
	FROM  T401 LEFT JOIN T501 ON T401.code = T501.code  and  T401.riqi = T501.jieshuriqi 
	WHERE  T501.jieshuriqi IS NOT NULL)	

,T599 AS (				
	SELECT A.kaishiriqi,B.jieshuriqi,A.code
	FROM T502 AS A INNER JOIN T503 AS B	ON A.code=B.code
	INNER JOIN T AS C ON C.code=B.code AND B.riqihao+1=C.riqihao
	WHERE B.pctChg>0 AND A.shou<B.shou  AND A.di<B.di AND B.val<0 AND B.shou>C.maxval)
	--SELECT * FROM T599 
	 
	
	--SELECT DISTINCT zuidalianxushangzhangshu,zuidadiehuozezuixiaozhang,zuidashou,suoyoumanzu,zhangdiezhouqishu,kaishiriqi,jieshuriqi,ISNULL(yangxianshu,0) AS yangxianshu,ISNULL(yinxianshu,0) AS yinxianshu,ISNULL(wushangyingxianfudushu,0) AS wushangyingxianfudushu,ISNULL(wuxiayingxianfudushu,0) AS wuxiayingxianfudushu,code
	INSERT INTO T10002([kaishiriqi],[jieshuriqi]   ,[code])
	SELECT DISTINCT  kaishiriqi,jieshuriqi,code		
	--INTO T10002
	FROM T599   
	--WHERE yangxianshu>=yinxianshu
	--ORDER BY zuidashou desc
	
	 
