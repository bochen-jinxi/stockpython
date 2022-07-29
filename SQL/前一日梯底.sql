 DELETE FROM T90
 go
declare @i int
set @i=1
while @i<=(SELECT COUNT(1) FROM dbo.lishijiager   WHERE riqi>='2022-01-01'  AND code='sh.601288')
begin  
 WITH T AS (SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi desc) AS sort, pctChg AS kbzf, pctChg AS kb, *  FROM dbo.lishijiager 
  WHERE riqi>='2022-01-01' 
  --AND code='sh.600009'
  )
,  T2 AS (
SELECT A0.code, A0.riqi , A0.kbzf AS k0zf, A0.kb AS k0, A0.shou AS  k0s,A0.kai AS k0k,A0.di AS k0d,A0.gao AS k0g,
A.kbzf AS k1zf, A.kb AS k1, A.shou AS  k1s,A.kai AS k1k,A.di AS k1d,A.gao AS k1g,
 B.kbzf AS k2zf,  B.kb AS k2, B.shou AS  k2s,B.kai AS k2k,B.di AS k2d,B.gao AS k2g,
A0.sort
  FROM T AS A  INNER JOIN  T AS A0 
ON  A.sort=A0.sort+1  
 AND A.code=A0.code
INNER JOIN  T AS B
ON  A.sort=B.sort-1  
 AND A.code=B.code
WHERE 
--A0.riqi='2022-01-27 00:00:00.000' and 
 A0.sort =@i
)
INSERT T90
SELECT * 
--INTO T90
 FROM T2 
WHERE 1=1
AND  T2.k0<0 AND T2.k1<0 AND  T2.k2<0  
AND T2.k0d*1.009>T2.k0s  AND T2.k1d*1.009>T2.k1s  AND T2.k2d*1.009>T2.k2s 
 
 --AND T2.riqi='2021-04-12 00:00:00.000'  
 
 
set @i=@i+1
end

--Ç°Ò»ÈÕÌÝµ×
SELECT  * FROM T90 
WHERE riqi='2022-01-27 00:00:00.000'
ORDER BY  code,riqi DESC