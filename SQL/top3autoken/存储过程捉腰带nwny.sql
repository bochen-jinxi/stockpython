USE stock
GO

-- 如果已存在则删除
IF OBJECT_ID('dbo.spnwny', 'P') IS NOT NULL
    DROP PROCEDURE dbo.spnwny
GO

CREATE PROCEDURE dbo.spnwny
    @StartDate DATE = NULL,      -- 开始日期，可选
    @EndDate   DATE = NULL,      -- 结束日期，可选
    @Days      INT  = 60         -- 默认最近60天
AS
BEGIN
    SET NOCOUNT ON;

    -- 清理临时表
    IF OBJECT_ID('tempdb..#T90') IS NOT NULL DROP TABLE #T90;

    -- 计算日期范围
    DECLARE @s DATE, @e DATE;
    IF @StartDate IS NULL OR @EndDate IS NULL
    BEGIN
        SET @e = ISNULL(@EndDate, CAST(GETDATE() AS DATE));
        SET @s = ISNULL(@StartDate, DATEADD(DAY, -@Days, @e));
    END
    ELSE
    BEGIN
        SET @s = @StartDate;
        SET @e = @EndDate;
    END

    -- 装载原始数据
    SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi DESC) AS riqihao,
           *
    INTO #T90
    FROM lishijiager
	--南网能源
--WHERE riqi>='2021-02-25' AND riqi<='2021-03-10' AND code='sz.003035' 
--应众互联
--WHERE riqi>='2020-04-01' AND riqi<='2020-04-15' AND code='sz.002464' 
    WHERE riqi >= @s AND riqi <= @e;

    ;WITH T AS (
        SELECT riqihao,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],
               [kai],[shou],[di],[gao],[chengjiaoliang],[pctChg],
               IIF(shou>=kai,shou,kai)as maxval,
               IIF(shou<=kai,shou,kai) AS minval				
        FROM #T90
        WHERE riqihao<=10
    )
    ,T3 AS ( 	 
        SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY shitifudu DESC) AS RowID,
               *,
               (gao/maxVal-1)*100 AS shangyingxianfudu,
               (minval/di-1) *100 AS xiayingxianfudu
        FROM T
    )
    ,T401 AS ( 
        SELECT T.*,
            CASE
                WHEN T.kai>A.shou THEN T.kai/A.shou-1
                WHEN T.kai<A.shou THEN 1-A.shou/T.kai
                ELSE 0
            END AS val  
        FROM T 
        INNER JOIN T AS A ON T.code = A.code 
        WHERE T.riqihao+1=A.riqihao
    )
    ,T402 AS ( 
        SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqihao DESC) AS RowID,* 
        FROM T401 
        WHERE val>0
    )
    ,T403 AS ( 
        SELECT * 
        FROM T402 
        WHERE RowID=1  AND riqihao>=9-3 AND val>0.02
    )
    ,T4 AS ( 
        SELECT * FROM T403
    )		
    ,T499 AS (
        SELECT COUNT(1) OVER (PARTITION BY T3.code) AS zhangdiezhouqishu,
               T3.[pctChg],T4.di AS kaishidi,T4.gao AS kaishigao,
               T4.code,T4.riqi AS kaishiriqi,T3.riqi,
               MAX(T3.riqi) OVER (PARTITION BY T3.code) AS jieshuriqi,
               T3.riqihao,T3.shitifudu,T4.riqihao AS zuigaojiariqihao,
               T3.riqihao AS zuihouriqihao,
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
        FROM T4 
        INNER JOIN T3 ON T4.code = T3.code AND T4.riqihao > T3.riqihao
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
        SELECT * FROM T9 WHERE zuidalianxushangzhangshu>= 2
    )
    ,T5 AS (
        SELECT *
        FROM  T10  
        WHERE kaishigao*1.25>zuidagao AND kaishidi/1.04<zuixiaodi
        AND  zuidashangyingxianfudu<8 AND zuidaxiayingxianfudu<6
    )	
    ,T590 AS (
        SELECT COUNT(1) OVER (PARTITION BY T5.code) AS suoyoumanzu ,* 
        FROM T5  
        WHERE (shangyingxianfudu<=4 AND xiayingxianfudu<=1)
           OR (shangyingxianfudu=0 OR  xiayingxianfudu=0)
    )
    ,T501 AS (
        SELECT DISTINCT code,kaishiriqi,jieshuriqi,yangxianshu,yinxianshu	
        FROM T590
        WHERE yangxianshu>=yinxianshu
    )
    ,T502 AS (
        SELECT T401.*,kaishiriqi 	
        FROM  T401 
        LEFT JOIN T501 ON T401.code = T501.code  and  T401.riqi = T501.kaishiriqi 
        WHERE  T501.kaishiriqi IS NOT NULL
    )
    ,T503 AS (
        SELECT T401.*,jieshuriqi 
        FROM  T401 
        LEFT JOIN T501 ON T401.code = T501.code  and  T401.riqi = T501.jieshuriqi 
        WHERE  T501.jieshuriqi IS NOT NULL
    )
    ,T599 AS (				
        SELECT A.kaishiriqi,B.jieshuriqi,A.code,B.val
        FROM T502 AS A 
        INNER JOIN T503 AS B ON A.code=B.code
        INNER JOIN T AS C ON C.code=B.code AND B.riqihao+1=C.riqihao
        WHERE  A.shou>B.shou AND A.di>B.di 
          AND  B.val<0 AND B.kai=B.di AND C.maxval>B.shou
    )	
    INSERT INTO T10001([kaishiriqi],[jieshuriqi],[code])
    SELECT DISTINCT kaishiriqi,jieshuriqi,code		
    FROM T599;
END
GO
