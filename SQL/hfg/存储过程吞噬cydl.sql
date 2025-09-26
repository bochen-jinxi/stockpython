USE stock
GO

IF OBJECT_ID('spcydl', 'P') IS NOT NULL
    DROP PROCEDURE spcydl
GO

CREATE PROCEDURE spcydl
    @StartDate DATE = NULL,      -- ��ʼ���ڣ���ѡ
    @EndDate   DATE = NULL,      -- �������ڣ���ѡ
    @Days      INT  = 60         -- Ĭ�����60��
AS
BEGIN
    SET NOCOUNT ON;

    -- ��̬ʱ�䷶Χ
    IF @StartDate IS NULL AND @EndDate IS NULL
    BEGIN
        SET @EndDate = CAST(GETDATE() AS DATE);
        SET @StartDate = DATEADD(DAY, -@Days, @EndDate);
    END
    ELSE IF @StartDate IS NULL
        SET @StartDate = DATEADD(DAY, -@Days, @EndDate);
    ELSE IF @EndDate IS NULL
        SET @EndDate = DATEADD(DAY, @Days, @StartDate);

    -- ��ʱ��
    IF OBJECT_ID('tempdb..#T90') IS NOT NULL DROP TABLE #T90;

    SELECT         ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi DESC) AS riqihao,          
		[code],[riqi],[kai], [shou],[di],
 		[gao], [chengjiaoliang],
		IIF(LEN(code)=5,( CASE WHEN ( shou - kai ) > 0 THEN 1
                               WHEN ( shou - kai ) = 0 THEN 0
                               WHEN ( shou - kai ) < 0 THEN -1
                          END ),[pctChg])		 AS  [pctChg]		
    INTO #T90
    FROM lishijiager
	--��Դ����
--WHERE riqi>='2021-02-26' AND riqi<='2021-03-10'  AND code='000966' 
--��������
--WHERE riqi>='2020-11-14' AND riqi<='2020-12-10' AND code='sz.300118' 
    WHERE riqi >= @StartDate AND riqi <=@EndDate;
 

    ;WITH T AS (
        SELECT riqihao,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],
               [kai],[shou],[di],[gao],[chengjiaoliang],[pctChg],
               IIF(shou>=kai,shou,kai)as maxval,
               IIF(shou<=kai,shou,kai) AS minval				
        FROM #T90
        WHERE riqihao<=9
    )
    ,T3 AS ( 	 
        SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY shitifudu DESC) AS RowID,
               *,
               (gao/maxVal-1)*100 AS shangyingxianfudu,
               (minval/di-1) *100 AS xiayingxianfudu
        FROM T
    )
    ,T401 AS ( 
        -- ������
        SELECT T.*,
               CASE
                    WHEN T.kai>A.shou THEN T.kai/A.shou-1 
                    WHEN T.kai<A.shou THEN 1-A.shou/T.kai 
                    ELSE 0 
               END AS val 
        FROM T INNER JOIN T AS A ON T.code = A.code 
        WHERE T.riqihao+1=A.riqihao
    )
    ,T402 AS ( 
        SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY val DESC) AS RowID,* 
        FROM T401 
        WHERE val>0
    )
    ,T4 AS ( 
        SELECT * 
        FROM T402 
        WHERE RowID=1  AND riqihao>=8-2 AND val>0.029
    )
    ,T499 AS (
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
        WHERE T4.RowID=1
    )
    ,T6	AS ( 
        SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS riqihaoasc,*
        FROM T499
    )
    ,T7 AS (
        SELECT riqihaoasc-ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS lianxuxiadieriqizu,*
        FROM T6
        WHERE pctChg>0
    )
    ,T8 AS (
        SELECT COUNT(1) OVER (PARTITION BY code, lianxuxiadieriqizu) AS lianxuxiadieshu,*
        FROM T7
    )
    ,T9 AS ( 		  
        SELECT MAX(lianxuxiadieshu) OVER (PARTITION BY code) AS zuidalianxushangzhangshu,*
        FROM T8
    )
    ,T10 AS ( 
        SELECT *
        FROM T9 
        WHERE zuidalianxushangzhangshu>= 2
    )
    ,T5 AS (
        SELECT *	
        FROM  T10  
        WHERE zuidagao/kaishigao-1<0.13  AND zuixiaodi/kaishidi-1<0.05
        AND  zuidashangyingxianfudu<4 AND zuidaxiayingxianfudu<4
    )
    ,T590 AS (
        SELECT COUNT(1) OVER (PARTITION BY T5.code) AS suoyoumanzu,* 
        FROM T5  
        WHERE (shangyingxianfudu<=4 AND xiayingxianfudu<=1)
        OR (shangyingxianfudu=0 OR  xiayingxianfudu=0)
    )
    ,T501 AS (
        SELECT DISTINCT code,kaishiriqi,jieshuriqi,yangxianshu,yinxianshu	
        FROM T590
        WHERE yangxianshu>yinxianshu
    )
    ,T502 AS (
        SELECT T401.*,kaishiriqi 	
        FROM  T401 LEFT JOIN T501 ON T401.code = T501.code  and  T401.riqi = T501.kaishiriqi 
        WHERE  T501.kaishiriqi IS NOT NULL
    )
    ,T503 AS (
        SELECT T401.*,jieshuriqi 
        FROM  T401 LEFT JOIN T501 ON T401.code = T501.code  and  T401.riqi = T501.jieshuriqi 
        WHERE  T501.jieshuriqi IS NOT NULL
    )
    ,T599 AS (				
        SELECT A.kaishiriqi,B.jieshuriqi,A.code
        FROM T502 AS A INNER JOIN T503 AS B	ON A.code=B.code
        INNER JOIN T AS C ON C.code=B.code AND B.riqihao+1=C.riqihao
        WHERE B.pctChg>0 AND A.shou<B.shou  AND B.di>A.di AND B.val<0 
          AND ((C.pctChg<0  AND B.shou>C.maxval AND B.kai<C.shou) 
            OR (C.pctChg>0  AND B.shou>C.maxval AND B.kai<C.kai))
    )
    INSERT INTO T10002([kaishiriqi],[jieshuriqi],[code])
    SELECT DISTINCT kaishiriqi,[jieshuriqi],code		
    FROM T599;
END
GO
