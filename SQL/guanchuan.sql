USE  stock

go

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
SELECT   * FROM B INNER JOIN A ON B.code = A.code
WHERE A.kai>A.shou  AND A.kai/1.02>A.shou   AND  ( A.shou>A.di*1.001 OR A.shou=A.di ) 
AND (B.kai<A.di OR B.kai< A.shou )    AND B.kai<B.shou AND (((A.kai-A.shou)/2)+A.shou)<B.shou
END