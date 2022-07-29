

 
--SELECT *  
--from lishijiager where code='sz.300223' and riqi>'2021-09-09'
--ORDER BY riqi desc
 
 DROP TABLE #T90
 go
;with T as (
SELECT *, Max(shou)
  OVER ( PARTITION BY code
    ORDER BY riqi
ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS H9,
	 MIN(shou)
  OVER ( PARTITION BY code
    ORDER BY riqi
ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) AS L9
from lishijiager 
where code='sz.300223' 
 )
 ,T2 as (
SELECT * ,((shou-L9)/(H9-L9)) * 100  as calcrsv from  T
where  H9-L9<>0  )
,T3 AS (
SELECT * , calcrsv*(1/3)+50*(2/3) AS  calck from  T2
)

SELECT *,ROW_NUMBER() OVER(PARTITION BY code ORDER BY riqi asc ) AS num  INTO #T90 FROM T3

SELECT * FROM #T90


declare @k DECIMAL(18,2)
declare @rsv DECIMAL(18,2)
declare @currentk DECIMAL(18,2)
declare @riqi DATETIME
declare @code NVARCHAR(20)
declare @i INT
set @i=1
--SELECT DISTINCT COUNT(1) OVER(PARTITION BY code),code   FROM #T90 
while @i<2
begin  
 SELECT @k=calck, @rsv=calcrsv, @riqi=riqi,@code=code  FROM  #T90 WHERE num=@i
 UPDATE lishijiager SET RSV=@rsv, K=@k WHERE riqi=@riqi AND code=@code
 SELECT @rsv=calcrsv, @currentk= calcrsv*1/3+@k*2/3,@riqi=riqi,@code=code FROM  #T90 WHERE num=@i+1
UPDATE lishijiager SET RSV=@rsv, K=@currentk WHERE riqi=@riqi AND code=@code
set @i=@i+1
end 