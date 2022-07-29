USE stock
GO


--²éÑ¯
DECLARE @intIDNum int
SET @intIDNum  = 100
WHILE @intIDNum > 0
BEGIN
			
	EXEC dbo.fanpu @days = @intIDNum -- int
	SET @intIDNum = @intIDNum - 1
END




DROP PROCEDURE fanpu
go

CREATE  PROCEDURE fanpu
(@days int)
AS
 BEGIN

--·´ÆË
WITH A AS(
SELECT * FROM dbo.lishijiage WHERE EXISTS(SELECT 1 FROM dbo.VCalendar WHERE  row=@days AND riqi=CDate)
)
,B AS(
SELECT * FROM dbo.lishijiage WHERE  EXISTS(SELECT 1 FROM dbo.VCalendar WHERE  row=(@days-1) AND riqi=CDate)
)
SELECT * FROM B INNER JOIN A ON B.code = A.code
WHERE A.kai>A.shou AND A.shou/1.001>A.di AND  A.shou/1.009<A.di AND  A.kai/1.04>A.shou AND A.kai/1.09<A.shou  
AND  B.kai>A.shou AND B.shou>A.gao AND B.kai<B.shou

END



DROP PROCEDURE yunxian
go

CREATE  PROCEDURE yunxian
(@days int)
AS 
BEGIN


--ÔÐÏß
 ;WITH A AS(
SELECT * FROM dbo.lishijiage WHERE EXISTS(SELECT 1 FROM dbo.VCalendar WHERE  row=@days AND riqi=CDate)
)
,B AS(
SELECT * FROM dbo.lishijiage WHERE  EXISTS(SELECT 1 FROM dbo.VCalendar WHERE  row=(@days-1) AND riqi=CDate)
)
SELECT  * FROM B INNER JOIN A ON B.code = A.code
WHERE A.kai>A.shou AND A.shou/1.001>A.di AND  A.shou/1.009<A.di AND  A.kai/1.04>A.shou AND A.kai/1.09<A.shou  
AND B.gao<A.gao AND  B.di<A.di   AND B.kai>A.shou  AND B.kai<B.shou

END


DROP PROCEDURE guanchuan
go

CREATE  PROCEDURE guanchuan
(@days int)
AS
 BEGIN

 --¹á´©
 ;WITH A AS(
SELECT * FROM dbo.lishijiage WHERE    EXISTS(SELECT 1 FROM dbo.VCalendar WHERE  row =@days  AND riqi=CDate)
)
,B AS(
SELECT * FROM dbo.lishijiage WHERE    EXISTS(SELECT 1 FROM dbo.VCalendar WHERE   row=(@days-1) AND riqi=CDate)
)
SELECT * FROM B INNER JOIN A ON B.code = A.code
WHERE A.kai>A.shou AND A.shou/1.001>A.di AND  A.shou/1.009<A.di AND A.kai/1.04>A.shou AND A.kai/1.09<A.shou  
AND B.kai<A.di  AND  A.gao/1.01>B.shou and  A.gao/1.04<B.shou AND B.kai<B.shou

END