---5分钟最大交量
 -----------------------------------------------------------------------------------
 --找最近8个交易日的K线
   use stock 
   go 
 
 WITH T AS (
 SELECT ROW_NUMBER() OVER(PARTITION BY  code ORDER BY chengjiaoliang DESC ) AS rowid,COUNT(1) OVER(PARTITION BY  code) AS rowid2, * FROM dbo.lishijiage5 
 WHERE   pctChg=0 AND riqi >='2021-10-25' AND  riqi<='2021-11-04'
 )
 SELECT * FROM T WHERE  rowid=1 AND rowid2>=5 AND riqi='2021-11-04'