USE stock;
GO

IF OBJECT_ID('dbo.sptddl', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sptddl;
GO

CREATE PROCEDURE dbo.sptddl
    @StartDate DATE = NULL,
    @EndDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- 默认60天
    IF @EndDate IS NULL
        SET @EndDate = CAST(GETDATE() AS DATE);

    IF @StartDate IS NULL
        SET @StartDate = DATEADD(DAY, -60, @EndDate);

    PRINT '执行区间：' + CONVERT(VARCHAR(10), @StartDate) + ' 到 ' + CONVERT(VARCHAR(10), @EndDate);

    -- 清理临时表
    IF OBJECT_ID('tempdb..#T90') IS NOT NULL DROP TABLE #T90;

    SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi DESC) AS riqihao, *
    INTO #T90
    FROM lishijiager
	--通达动力
--WHERE riqi >='2021-02-26' and riqi <='2021-03-19' AND code='002576' 
    WHERE riqi >= @StartDate AND riqi <= @EndDate;

    ;WITH T AS (
        SELECT 
            riqihao, (shou - kai) / kai * 100 AS shitifudu,
            code, riqi, kai, shou, di, gao, chengjiaoliang, pctChg,
            IIF(shou >= kai, shou, kai) AS maxval,
            IIF(shou <= kai, shou, kai) AS minval
        FROM #T90
        WHERE riqihao <= 16
    ),
    T3 AS (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY code ORDER BY shitifudu DESC) AS RowID,
            *, 
            (gao / maxval - 1) * 100 AS shangyingxianfudu,
            (minval / di - 1) * 100 AS xiayingxianfudu
        FROM T
    ),
    T401 AS (
        SELECT T.*,
            CASE
                WHEN T.kai > A.shou THEN T.kai / A.shou - 1
                WHEN T.kai < A.shou THEN 1 - A.shou / T.kai
                ELSE 0
            END AS val
        FROM T INNER JOIN T AS A 
        ON T.code = A.code AND T.riqihao + 1 = A.riqihao
    ),
    T402 AS (
        SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY val DESC) AS RowID, *
        FROM T401
        WHERE val > 0
    ),
    T4 AS (
        SELECT *
        FROM T402
        WHERE RowID = 1 AND riqihao >= 15-3 AND val > 0.018
    ),
    T499 AS (
        SELECT 
            COUNT(1) OVER (PARTITION BY T3.code) AS zhangdiezhouqishu,
            T3.pctChg,
            T4.di AS kaishidi,
            T4.gao AS kaishigao,
            T4.code,
            T4.riqi AS kaishiriqi,
            T3.riqi,
            MAX(T3.riqi) OVER (PARTITION BY T3.code) AS jieshuriqi,
            T3.riqihao,
            T3.shitifudu,
            T4.riqihao AS zuigaojiariqihao,
            T3.riqihao AS zuihouriqihao,
            T3.di, T3.kai, T3.shou, T3.gao,
            COUNT(CASE WHEN T3.shangyingxianfudu = 0 THEN 1 END) OVER (PARTITION BY T3.code) AS wushangyingxianfudushu,
            COUNT(CASE WHEN T3.xiayingxianfudu = 0 THEN 1 END) OVER (PARTITION BY T3.code) AS wuxiayingxianfudushu,
            COUNT(CASE WHEN T3.pctChg >= 0 THEN 1 END) OVER (PARTITION BY T3.code) AS yangxianshu,
            COUNT(CASE WHEN T3.pctChg < 0 THEN 1 END) OVER (PARTITION BY T3.code) AS yinxianshu,
            MIN(T3.pctChg) OVER (PARTITION BY T3.code) AS zuidadiehuozezuixiaozhang,
            MAX(T3.gao) OVER (PARTITION BY T3.code) AS zuidagao,
            MIN(T3.di) OVER (PARTITION BY T3.code) AS zuixiaodi,
            MAX(T3.shou) OVER (PARTITION BY T3.code) AS zuidashou,
            MAX(T3.shangyingxianfudu) OVER (PARTITION BY T3.code) AS zuidashangyingxianfudu,
            MAX(T3.xiayingxianfudu) OVER (PARTITION BY T3.code) AS zuidaxiayingxianfudu,
            T3.shangyingxianfudu,
            T3.xiayingxianfudu
        FROM T4 INNER JOIN T3 ON T4.code = T3.code AND T4.riqihao > T3.riqihao
        WHERE T4.RowID = 1
    ),
    T6 AS (
        SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS riqihaoasc, *
        FROM T499
    ),
    T7 AS (
        SELECT riqihaoasc - ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS lianxuxiadieriqizu, *
        FROM T6
        WHERE pctChg >= 0
    ),
    T8 AS (
        SELECT COUNT(1) OVER (PARTITION BY code, lianxuxiadieriqizu) AS lianxuxiadieshu, *
        FROM T7
    ),
    T9 AS (
        SELECT MAX(lianxuxiadieshu) OVER (PARTITION BY code) AS zuidalianxushangzhangshu, *
        FROM T8
    ),
    T10 AS (
        SELECT *
        FROM T9
        WHERE zuidalianxushangzhangshu >= 4
    ),
    T5 AS (
        SELECT *
        FROM T10
        WHERE zuidagao / kaishigao - 1 < 0.05 
          AND ABS(1 - kaishidi / zuixiaodi) < 0.09
          AND zuidashangyingxianfudu < 7 
          AND zuidaxiayingxianfudu < 4
    ),
    T590 AS (
        SELECT COUNT(1) OVER (PARTITION BY T5.code) AS suoyoumanzu, * 
        FROM T5  
        WHERE (shangyingxianfudu <= 2 AND xiayingxianfudu <= 2)
           OR (shangyingxianfudu = 0 OR xiayingxianfudu = 0)
    ),
    T501 AS (
        SELECT DISTINCT code, kaishiriqi, jieshuriqi, yangxianshu, yinxianshu
        FROM T590
        WHERE yangxianshu > yinxianshu
    ),
    T502 AS (
        SELECT T3.*, kaishiriqi
        FROM T3 
        LEFT JOIN T501 ON T3.code = T501.code AND T3.riqi = T501.kaishiriqi
        WHERE T501.kaishiriqi IS NOT NULL
    ),
    T503 AS (
        SELECT T3.*, jieshuriqi
        FROM T3 
        LEFT JOIN T501 ON T3.code = T501.code AND T3.riqi = T501.jieshuriqi
        WHERE T501.jieshuriqi IS NOT NULL
    ),
    T599 AS (
        SELECT A.kaishiriqi, B.jieshuriqi, A.code
        FROM T502 AS A 
        INNER JOIN T503 AS B ON A.code = B.code
        WHERE 
            B.pctChg > 0 AND 
            A.shou < B.shou AND 
            ABS(1 - A.shou / B.di) < 0.019
    )

    -- 插入最终结果
    INSERT INTO T10000 ([kaishiriqi], [jieshuriqi], [code])
    SELECT DISTINCT kaishiriqi, jieshuriqi, code
    FROM T599
	   PRINT '✅ 执行完成，结果写入 T10000';
END;
GO
