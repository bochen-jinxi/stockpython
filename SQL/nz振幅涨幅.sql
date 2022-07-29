 


USE stock
GO
------------------------------------------------------通达信统计 涨幅 2020-12-29  到 2020-01-08 ----------------------------------------------------------
----------------------------------------------------------
-------------------------------------通达信统计 涨幅 2020-12-29  到 2020-01-08
---------------------------通达信统计 涨幅 2020-12-29  到 2020-01-08
-----------------------------通达信统计 涨幅 2020-12-29  到 2020-01-08
--------------------------------------------------------------------------------
WITH  T AS (SELECT * FROM dbo.lishijiage WHERE riqi >='2020-12-28')
, T1 AS ( SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY  riqi) AS rid, * FROM T  
--WHERE code='sh.603195'
) 
--SELECT * FROM T1
SELECT ((B.shou-A.shou)/A.shou)*100, * FROM T1 AS A  INNER JOIN  T1 AS B ON A.code = B.code  AND a.rid+8=b.rid
 WHERE (((B.shou-A.shou)/A.shou)*100)>15
  AND (((B.shou-A.shou)/A.shou)*100)<15.2   
 ORDER BY 1 DESC 



------------------------------------------------------通达信统计 振幅 2020-12-29  到 2020-01-08 ----------------------------------------------------------
----------------------------------------------------------
-------------------------------------通达信统计 振幅 2020-12-29  到 2020-01-08
---------------------------通达信统计 振幅 2020-12-29  到 2020-01-08
-----------------------------通达信统计 振幅 2020-12-29  到 2020-01-08
--------------------------------------------------------------------------------
WITH  T AS (SELECT * FROM dbo.lishijiage WHERE riqi >='2020-12-17' AND riqi<='2020-12-25')
, T1 AS ( SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY  riqi) AS rid, * FROM T  
--WHERE code='sh.603882'
) 
--SELECT * FROM T1
,T2 AS  (SELECT MAX(A.gao) OVER(PARTITION BY A.code) AS maxval,MIN(A.di) OVER(PARTITION BY A.code) AS minval, * FROM T1  AS A)
SELECT (((B.maxval-B.minval)/B.minval)*100), * FROM  T2 AS B
 WHERE (((B.maxval-B.minval)/B.minval)*100)>15
  --AND (((B.shou-A.shou)/A.shou)*100)<15.2
   
 ORDER BY 1 DESC 




 