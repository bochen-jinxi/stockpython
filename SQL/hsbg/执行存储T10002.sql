USE [stock]
GO

DECLARE @CurrentDate DATE = '2025-10-16'
DECLARE @Counter INT = 0
DECLARE @TotalExecutions INT = 1  -- 总共执行次数

WHILE @Counter < @TotalExecutions
BEGIN
    PRINT '开始执行日期: ' + CAST(@CurrentDate AS VARCHAR(20)) + '，第 ' + CAST(@Counter + 1 AS VARCHAR) + ' 次循环'
    
    BEGIN TRY
        -- 执行第一个存储过程
        PRINT '  执行 [dbo].[spcydl]...'
        EXEC [dbo].[spcydl]
            @EndDate = @CurrentDate,
            @Days = 30

        -- 执行第二个存储过程
        PRINT '  执行 [dbo].[sphhkj]...'
        EXEC [dbo].[sphhkj]
            @EndDate = @CurrentDate,
            @Days = 30

        -- 执行第三个存储过程
        PRINT '  执行 [dbo].[sptqly]...'
        EXEC [dbo].[sptqly]
            @EndDate = @CurrentDate,
            @Days = 30

        -- 执行第四个存储过程
        PRINT '  执行 [dbo].[spdfrs]...'
        EXEC [dbo].[spdfrs]
            @EndDate = @CurrentDate,
            @Days = 30
            
        PRINT '  完成日期: ' + CAST(@CurrentDate AS VARCHAR(20))
        
    END TRY
    BEGIN CATCH
        PRINT '错误发生在日期: ' + CAST(@CurrentDate AS VARCHAR(20))
        PRINT '错误信息: ' + ERROR_MESSAGE()
        -- 可以选择继续执行或停止
        -- BREAK;  -- 取消注释此行会在出错时停止循环
    END CATCH

    -- 日期递减1天
    SET @CurrentDate = DATEADD(DAY, -1, @CurrentDate)
    SET @Counter = @Counter + 1
    
  
END

PRINT '所有存储过程执行完成！'
GO
 
-- USE [stock]
--GO
--EXEC	[dbo].spcydl
--GO
--EXEC	[dbo].sphhkj
--GO
--EXEC	[dbo].sptqly
--GO
--EXEC	[dbo].spdfrs
--GO

--DELETE FROM dbo.T10002

SELECT * FROM       dbo.T10002  INNER JOIN dbo.lishijiager  ON dbo.T10002.code = dbo.lishijiager.code
 
ORDER BY shou DESC 