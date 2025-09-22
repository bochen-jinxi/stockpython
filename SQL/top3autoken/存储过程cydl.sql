USE stock
GO

IF OBJECT_ID('dbo.spcydl', 'P') IS NOT NULL
    DROP PROCEDURE dbo.spcydl
GO

CREATE PROCEDURE dbo.spcydl
(
    @StartDate DATE = NULL,   -- 开始日期
    @EndDate   DATE = NULL    -- 结束日期
)
AS
BEGIN
    SET NOCOUNT ON;

    -- 默认值: 最近60天
    IF @EndDate IS NULL
        SET @EndDate = CAST(GETDATE() AS DATE);

    IF @StartDate IS NULL
        SET @StartDate = DATEADD(DAY, -60, @EndDate);

    PRINT '开始日期: ' + CONVERT(VARCHAR(10), @StartDate, 120);
    PRINT '结束日期: ' + CONVERT(VARCHAR(10), @EndDate, 120);

    -- 清理旧的T90表
    IF OBJECT_ID('dbo.T90', 'U') IS NOT NULL
        DROP TABLE dbo.T90;

    -- 生成T90
    SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi DESC) AS riqihao, *
    INTO dbo.T90
    FROM lishijiager
    WHERE riqi >= @StartDate AND riqi <= @EndDate;

    ;WITH T AS (
        SELECT riqihao,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],
            [kai],[shou],[di],[gao],[chengjiaoliang],[pctChg],
            IIF(shou>=kai,shou,kai) as maxval,
            IIF(shou<=kai,shou,kai) AS minval				
        FROM T90
        WHERE riqihao <= 9
    )
    -- … 这里中间所有CTE逻辑保持不变 …
    ,T599 AS (				
        SELECT A.kaishiriqi,B.jieshuriqi,A.code
        FROM T502 AS A 
        INNER JOIN T503 AS B ON A.code=B.code
        INNER JOIN T AS C ON C.code=B.code AND B.riqihao+1=C.riqihao
        WHERE B.pctChg>0 
          AND A.shou<B.shou  
          AND B.di>A.di 
          AND B.val<0 
          AND (
              (C.pctChg<0 AND B.shou>C.maxval AND B.kai<C.shou) 
              OR 
              (C.pctChg>0 AND B.shou>C.maxval AND B.kai<C.kai)
          )
    )
    INSERT INTO T10002([kaishiriqi],[jieshuriqi],[code])
    SELECT DISTINCT kaishiriqi,[jieshuriqi],code		
    FROM T599; 
END
GO
