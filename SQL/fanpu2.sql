USE  stock
GO
 
  WITH  T AS (

SELECT DISTINCT t3.* FROM dbo.lishijiage AS t  
CROSS APPLY
(SELECT * FROM  lishijiage AS t2  WHERE t2.code=t.code
ORDER BY   t2.riqi DESC 
OFFSET 0 ROWS FETCH NEXT 2 ROWS ONLY) AS t3)
,T2 AS (
SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi desc) AS rid,* FROM T
-- WHERE code='sz.300788' 
  )
  , T3 AS ( 
  SELECT * FROM T2 WHERE rid=1
  )
  ,T4 AS (
  SELECT * FROM T2 WHERE rid=2
  )
  SELECT * FROM T3 AS a INNER JOIN  T4 AS b ON  a.code=b.code 
  WHERE a.kai>b.shou AND a.shou>b.shou
 
