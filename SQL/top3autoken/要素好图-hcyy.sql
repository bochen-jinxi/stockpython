 
CREATE PROCEDURE sp_分析最近NDays好图_Debug
    @RecentDays INT = 30,
	@EndDate DATE = NULL               -- 分析截止日期（默认 = 数据中最大日期）
AS
BEGIN
    SET NOCOUNT ON;

    -- 清理临时表
    IF OBJECT_ID('tempdb..#T90') IS NOT NULL DROP TABLE #T90;
    IF OBJECT_ID('tempdb..#T') IS NOT NULL DROP TABLE #T;
    IF OBJECT_ID('tempdb..#T3') IS NOT NULL DROP TABLE #T3;
    IF OBJECT_ID('tempdb..#T4') IS NOT NULL DROP TABLE #T4;
    IF OBJECT_ID('tempdb..#T499') IS NOT NULL DROP TABLE #T499;
    IF OBJECT_ID('tempdb..#T6') IS NOT NULL DROP TABLE #T6;
    IF OBJECT_ID('tempdb..#T7') IS NOT NULL DROP TABLE #T7;
    IF OBJECT_ID('tempdb..#T8') IS NOT NULL DROP TABLE #T8;
    IF OBJECT_ID('tempdb..#T9') IS NOT NULL DROP TABLE #T9;
    IF OBJECT_ID('tempdb..#T10') IS NOT NULL DROP TABLE #T10;
    IF OBJECT_ID('tempdb..#T5') IS NOT NULL DROP TABLE #T5;
    IF OBJECT_ID('tempdb..#T590') IS NOT NULL DROP TABLE #T590;
    IF OBJECT_ID('tempdb..#T501') IS NOT NULL DROP TABLE #T501;
    IF OBJECT_ID('tempdb..#T502') IS NOT NULL DROP TABLE #T502;
    IF OBJECT_ID('tempdb..#T503') IS NOT NULL DROP TABLE #T503;
    IF OBJECT_ID('tempdb..#T599') IS NOT NULL DROP TABLE #T599;
    IF OBJECT_ID('T10000') IS NOT NULL DROP TABLE T10000;

	 -- 自动默认设置为 MAX(riqi)
    IF @EndDate IS NULL
        SELECT @EndDate = MAX(riqi) FROM lishijiager;

    -- 步骤1：获取最近N个交易日的股票数据
    SELECT *
    INTO #T90
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi DESC) AS riqihao
        FROM lishijiager
       WHERE riqi <= @EndDate AND riqi >= DATEADD(DAY, -60, @EndDate)
    ) AS temp
    WHERE riqihao <= @RecentDays;

	--SELECT @EndDate,  DATEADD(DAY, -60, @EndDate)  ;
	
	--SELECT * FROM #T90; 
    -- 步骤2：基础计算（实体幅度）
    SELECT
        riqihao,
        (shou - kai) / kai * 100 AS shitifudu,
        code, riqi, kai, shou, di, gao, chengjiaoliang, pctChg,
        IIF(shou >= kai, shou, kai) AS maxval,
        IIF(shou <= kai, shou, kai) AS minval
    INTO #T
    FROM #T90
    WHERE riqihao <= 20;

	--SELECT * FROM #T; 

    -- 步骤3：实体排序 + 上下影线
    SELECT
        ROW_NUMBER() OVER (PARTITION BY code ORDER BY shitifudu DESC) AS RowID,
        *,
        (gao / maxval - 1) * 100 AS shangyingxianfudu,
        (minval / di - 1) * 100 AS xiayingxianfudu
    INTO #T3
    FROM #T;

    -- 步骤4：找最大实体日
    SELECT *
    INTO #T4
    FROM #T3
    WHERE RowID = 1 AND riqihao >= 16;

    -- 步骤5：找最大实体之后的分析周期
    SELECT
        COUNT(1) OVER (PARTITION BY t3.code) AS zhangdiezhouqishu, t3.pctChg,
        t4.di AS kaishidi, t4.gao AS kaishigao, t4.code, t4.riqi AS kaishiriqi, t3.riqi,
        MAX(t3.riqi) OVER (PARTITION BY t3.code) AS jieshuriqi,
        t3.riqihao, t3.shitifudu, t4.riqihao AS zuigaojiariqihao, t3.riqihao AS zuihouriqihao,
        t3.di, t3.kai, t3.shou, t3.gao,
        COUNT(CASE WHEN t3.shangyingxianfudu = 0 THEN 1 END) OVER(PARTITION BY t3.code) AS wushangyingxianfudushu,
        COUNT(CASE WHEN t3.xiayingxianfudu = 0 THEN 1 END) OVER(PARTITION BY t3.code) AS wuxiayingxianfudushu,
        COUNT(CASE WHEN t3.pctChg >= 0 THEN 1 END) OVER(PARTITION BY t3.code) AS yangxianshu,
        COUNT(CASE WHEN t3.pctChg < 0 THEN 1 END) OVER(PARTITION BY t3.code) AS yinxianshu,
        MIN(t3.pctChg) OVER(PARTITION BY t3.code) AS zuidadiehuozezuixiaozhang,
        MAX(t3.gao) OVER(PARTITION BY t3.code) AS zuidagao,
        MIN(t3.di) OVER(PARTITION BY t3.code) AS zuixiaodi,
        MAX(t3.shou) OVER(PARTITION BY t3.code) AS zuidashou,
        MAX(t3.shangyingxianfudu) OVER(PARTITION BY t3.code) AS zuidashangyingxianfudu,
        MAX(t3.xiayingxianfudu) OVER(PARTITION BY t3.code) AS zuidaxiayingxianfudu,
        t3.shangyingxianfudu, t3.xiayingxianfudu
    INTO #T499
    FROM #T3 t3
    INNER JOIN #T4 t4 ON t3.code = t4.code AND t4.riqihao > t3.riqihao
    WHERE t4.RowID = 1;

    -- 步骤6：顺序编号
    SELECT
        ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS riqihaoasc,
        *
    INTO #T6
    FROM #T499;

    -- 步骤7：连续阳线组
    SELECT
        riqihaoasc - ROW_NUMBER() OVER (PARTITION BY code ORDER BY riqi) AS lianxuxiadieriqizu,
        *
    INTO #T7
    FROM #T6
    WHERE pctChg > 0;

    -- 步骤8：统计连续阳线
    SELECT
 COUNT(1) OVER (PARTITION BY code, lianxuxiadieriqizu) AS lianxuxiadieshu,
        *
    INTO #T8
    FROM #T7;

    -- 步骤9：找最大连续阳线
    SELECT
        MAX(lianxuxiadieshu) OVER (PARTITION BY code) AS zuidalianxushangzhangshu,
        *
    INTO #T9
    FROM #T8;

    -- 步骤10：保留连续阳线 ≥ 6
    SELECT *
    INTO #T10
    FROM #T9
    WHERE zuidalianxushangzhangshu >= 6;

    -- 步骤11：过滤上下影线幅度
    SELECT *
    INTO #T5
    FROM #T10
    WHERE zuidagao / kaishigao - 1 < 0.10
      AND ABS(1 - kaishidi / zuixiaodi) < 0.10
      AND zuidashangyingxianfudu < 5
      AND zuidaxiayingxianfudu < 5;

    -- 步骤12：进一步筛选光头光脚
    SELECT COUNT(1) OVER (PARTITION BY code) AS suoyoumanzu, *
    INTO #T590
    FROM #T5
    WHERE (shangyingxianfudu <= 0.5 AND xiayingxianfudu <= 0.5)
       OR (shangyingxianfudu = 0 OR xiayingxianfudu = 0);

    -- 步骤13：只保留阳线多于阴线的
    SELECT DISTINCT code, kaishiriqi, jieshuriqi, yangxianshu, yinxianshu
    INTO #T501
    FROM #T590
    WHERE yangxianshu > yinxianshu;

    -- 步骤14：找到开始日、结束日详细行
    SELECT t3.*, kaishiriqi
    INTO #T502
    FROM #T3 t3
    JOIN #T501 ON t3.code = #T501.code AND t3.riqi = #T501.kaishiriqi;

    SELECT t3.*, jieshuriqi
    INTO #T503
    FROM #T3 t3
    JOIN #T501 ON t3.code = #T501.code AND t3.riqi = #T501.jieshuriqi;

    -- 步骤15：最终筛选条件
    SELECT A.kaishiriqi, B.jieshuriqi, A.code
    INTO #T599
    FROM #T502 A
    JOIN #T503 B ON A.code = B.code
    WHERE B.pctChg > 0
      AND B.di = B.kai
      AND ABS(1 - A.di / B.di) < 0.02
      AND A.gao / B.gao - 1 < 0.02;

    -- 输出最终结果
    SELECT DISTINCT kaishiriqi, jieshuriqi, code
    INTO T10000
    FROM #T599;

  SELECT * FROM T10000

END;
