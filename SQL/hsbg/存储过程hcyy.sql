USE stock;
GO

IF OBJECT_ID('dbo.sphcyy', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sphcyy;
GO

CREATE PROCEDURE dbo.sphcyy
    @StartDate DATE = NULL,
    @EndDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- 如果日期为 NULL，自动设为最近60天
    IF @EndDate IS NULL
        SET @EndDate = CAST(GETDATE() AS DATE);

    IF @StartDate IS NULL
        SET @StartDate = DATEADD(DAY, -60, @EndDate);

    PRINT '执行日期范围：' + CONVERT(VARCHAR(10), @StartDate) + ' 到 ' + CONVERT(VARCHAR(10), @EndDate);

    -- 删除临时表（避免冲突）
    IF OBJECT_ID('tempdb..#T90') IS NOT NULL DROP TABLE #T90;

    -- Step 1: 取出最近60天的历史数据并编号
    SELECT 
               ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi DESC) AS riqihao,          
		[code],[riqi],[kai], [shou],[di],
 		[gao], [chengjiaoliang],
		IIF(LEN(code)=5,( CASE WHEN ( shou - kai ) > 0 THEN 1
                               WHEN ( shou - kai ) = 0 THEN 0
                               WHEN ( shou - kai ) < 0 THEN -1
                          END ),[pctChg])		 AS  [pctChg]		
    INTO #T90
    FROM lishijiager
	--海辰药业
--WHERE riqi>='2018-02-28' AND riqi<='2018-03-26' AND code='300584' 
    WHERE riqi >= @StartDate AND riqi <= @EndDate;
   
    -- 后续逻辑不变（与前面存储过程一致）
    ;WITH T AS (
        SELECT 
            riqihao,
            (shou - kai) / kai * 100 AS shitifudu,
            [code], [riqi], [kai], [shou], [di], [gao], [chengjiaoliang], [pctChg],
            IIF(shou >= kai, shou, kai) AS maxval,
            IIF(shou <= kai, shou, kai) AS minval
        FROM #T90
        WHERE riqihao <= 20
    )
	,
    T3 AS (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY code ORDER BY shitifudu DESC) AS RowID,
            *,
            (gao / maxVal - 1) * 100 AS shangyingxianfudu,
            (minval / di - 1) * 100 AS xiayingxianfudu
        FROM T
    ),
    T4 AS (
        SELECT *
        FROM T3
        WHERE RowID = 1 AND riqihao >= 19-3
    ),
    T499 AS (
        SELECT 
            COUNT(1) OVER (PARTITION BY T3.code) AS zhangdiezhouqishu,
            T3.[pctChg],
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
            COUNT(CASE WHEN T3.shangyingxianfudu = 0 THEN 1 END) OVER(PARTITION BY T3.code) AS wushangyingxianfudushu,
            COUNT(CASE WHEN T3.xiayingxianfudu = 0 THEN 1 END) OVER(PARTITION BY T3.code) AS wuxiayingxianfudushu,
            COUNT(CASE WHEN T3.pctChg >= 0 THEN 1 END) OVER(PARTITION BY T3.code) AS yangxianshu,
            COUNT(CASE WHEN T3.pctChg < 0 THEN 1 END) OVER(PARTITION BY T3.code) AS yinxianshu,
            MIN(T3.[pctChg]) OVER (PARTITION BY T3.code) AS zuidadiehuozezuixiaozhang,
            MAX(T3.gao) OVER (PARTITION BY T3.code) AS zuidagao,
            MIN(T3.di) OVER (PARTITION BY T3.code) AS zuixiaodi,
            MAX(T3.shou) OVER (PARTITION BY T3.code) AS zuidashou,
            MAX(T3.shangyingxianfudu) OVER (PARTITION BY T3.code) AS zuidashangyingxianfudu,
            MAX(T3.xiayingxianfudu) OVER (PARTITION BY T3.code) AS zuidaxiayingxianfudu,
            T3.shangyingxianfudu,
            T3.xiayingxianfudu
        FROM T4 
        INNER JOIN T3 ON T4.code = T3.code AND T4.riqihao > T3.riqihao
        WHERE T4.RowID = 1
    ),
    T6 AS (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS riqihaoasc,
            *
        FROM T499
    ),
    T7 AS (
        SELECT 
            riqihaoasc - ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS lianxuxiadieriqizu,
            *
        FROM T6
        WHERE pctChg > 0
    ),
    T8 AS (
        SELECT 
            COUNT(1) OVER (PARTITION BY code, lianxuxiadieriqizu) AS lianxuxiadieshu,
            *
        FROM T7
    ),
    T9 AS (
        SELECT 
            MAX(lianxuxiadieshu) OVER (PARTITION BY code) AS zuidalianxushangzhangshu,
            *
        FROM T8
    ),
    T10 AS (
        SELECT *
        FROM T9
        WHERE zuidalianxushangzhangshu >= 6
    ),
    T5 AS (
        SELECT *
        FROM T10
        WHERE 
            zuidagao / kaishigao - 1 < 0.10 AND 
            ABS(1 - kaishidi / zuixiaodi) < 0.10 AND 
            zuidashangyingxianfudu < 5 AND 
            zuidaxiayingxianfudu < 5
    ),
    T590 AS (
        SELECT 
            COUNT(1) OVER (PARTITION BY T5.code) AS suoyoumanzu,
            * 
        FROM T5  
        WHERE 
            (shangyingxianfudu <= 0.5 AND xiayingxianfudu <= 0.5)
            OR (shangyingxianfudu = 0 OR xiayingxianfudu = 0)
    ),
    T501 AS (
        SELECT DISTINCT 
            code, kaishiriqi, jieshuriqi, yangxianshu, yinxianshu
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
        SELECT 
            A.kaishiriqi,
            B.jieshuriqi,
            A.code
        FROM T502 AS A 
        INNER JOIN T503 AS B ON A.code = B.code
        WHERE 
            B.pctChg > 0 AND 
            B.di = B.kai AND 
            ABS(1 - A.di / B.di) < 0.02 AND 
            A.gao / B.gao - 1 < 0.03
    )
    
    -- 插入最终结果
    INSERT INTO T10000 ([kaishiriqi], [jieshuriqi], [code],[name])
    SELECT DISTINCT kaishiriqi, jieshuriqi, code,'sphcyy'
    FROM T599;

    PRINT '? 筛选完成，已将结果写入 T10000';

END;
GO
