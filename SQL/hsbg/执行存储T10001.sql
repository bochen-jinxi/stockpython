USE [stock]
GO

DECLARE @CurrentDate DATE = '2025-10-16'
DECLARE @Counter INT = 0
DECLARE @TotalExecutions INT = 1  -- �ܹ�ִ�д���

WHILE @Counter < @TotalExecutions
BEGIN
    PRINT '��ʼִ������: ' + CAST(@CurrentDate AS VARCHAR(20)) + '���� ' + CAST(@Counter + 1 AS VARCHAR) + ' ��ѭ��'
    
    BEGIN TRY
        -- ִ�е�һ���洢����
        PRINT '  ִ�� [dbo].[splskj]...'
        EXEC [dbo].[splskj]
            @EndDate = @CurrentDate,
            @Days = 30

        -- ִ�еڶ����洢����
        PRINT '  ִ�� [dbo].[spnwny]...'
        EXEC [dbo].[spnwny]
            @EndDate = @CurrentDate,
            @Days = 30

        -- ִ�е������洢����
        PRINT '  ִ�� [dbo].[spyzhl]...'
        EXEC [dbo].[spyzhl]
            @EndDate = @CurrentDate,
            @Days = 30

        -- ִ�е��ĸ��洢����
        PRINT '  ִ�� [dbo].[spsqhy]...'
        EXEC [dbo].[spsqhy]
            @EndDate = @CurrentDate,
            @Days = 30
            
        PRINT '  �������: ' + CAST(@CurrentDate AS VARCHAR(20))
        
    END TRY
    BEGIN CATCH
        PRINT '������������: ' + CAST(@CurrentDate AS VARCHAR(20))
        PRINT '������Ϣ: ' + ERROR_MESSAGE()
        -- ����ѡ�����ִ�л�ֹͣ
        -- BREAK;  -- ȡ��ע�ʹ��л��ڳ���ʱֹͣѭ��
    END CATCH

    -- ���ڵݼ�1��
    SET @CurrentDate = DATEADD(DAY, -1, @CurrentDate)
    SET @Counter = @Counter + 1
    
  
END

PRINT '���д洢����ִ����ɣ�'
GO
 
 --DELETE FROM dbo.T10001

-- USE [stock]
--GO
--EXEC	[dbo].splskj
--GO
--EXEC	[dbo].spnwny
--GO
--EXEC	[dbo].spyzhl
--GO
--EXEC	[dbo].spsqhy
--GO

--DELETE FROM dbo.T10001

SELECT * FROM       dbo.T10001  INNER JOIN dbo.lishijiager  ON dbo.T10001.code = dbo.lishijiager.code

ORDER BY shou DESC 
