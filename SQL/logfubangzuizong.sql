USE stock
GO
--DROP TABLE TEMPA99
--go
--SELECT lishijiage.*, [row]
--INTO TEMPA99 
--FROM dbo.lishijiage INNER JOIN dbo.VCalendar ON riqi=CDate


 DROP TABLE TEMPA100

SELECT        TEMPA99.*,row+1 AS [1dayriqi],
row+2 AS [2dayriqi],
row+3 AS [3dayriqi],
row+4 AS [4dayriqi],
row+5 AS [5dayriqi],
dbo.lishilonghubang.trade
INTO  TEMPA100 FROM TEMPA99 INNER JOIN dbo.lishilonghubang ON TEMPA99.code=lishilonghubang.code AND lishilonghubang.CTime=TEMPA99.riqi
WHERE dbo.lishilonghubang.code='002881' 

SELECT TEMPA100.*,A.shou,
B.shou,
C.shou,
D.shou,
E.shou



 FROM TEMPA100  INNER   JOIN  TEMPA99 AS A ON  A.code=TEMPA100.code AND TEMPA100.[1dayriqi]=A.[row] 
INNER   JOIN  TEMPA99 AS B ON  B.code=TEMPA100.code AND TEMPA100.[2dayriqi]=B.[row] 
INNER   JOIN  TEMPA99 AS C ON  C.code=TEMPA100.code AND TEMPA100.[3dayriqi]=C.[row] 
INNER   JOIN  TEMPA99 AS D ON  D.code=TEMPA100.code AND TEMPA100.[4dayriqi]=D.[row] 
INNER   JOIN  TEMPA99 AS E ON  E.code=TEMPA100.code AND TEMPA100.[5dayriqi]=E.[row] 


 DROP TABLE TEMPA
 go
 --龙虎榜营业部买入股票
SELECT TOP 100 Percent  lishilonghubang.*, riqi, row, row+5 AS [5day],row+10 AS[10day],row+20 AS[20day],row+60 AS [60day], lishijiage.shou  
--INTO TEMPA 
FROM dbo.lishijiage INNER JOIN dbo.VCalendar ON riqi=CDate
INNER  JOIN dbo.lishilonghubang  ON dbo.lishijiage.code=dbo.lishilonghubang.code AND CTime=riqi
--WHERE lishilonghubang.Sales='中国中投证券有限责任公司无锡清扬路证券营业部' 
WHERE dbo.lishilonghubang.code='000633'
ORDER BY riqi ASC

 SELECT * FROM  dbo.lishijiage WHERE dbo.lishijiage.code='000633' 
 

-- DROP TABLE TEMPB

-- go
-- --所以股票的历史价格
-- SELECT TOP 100  Percent  code,  riqi, row, row+5 AS [5day],row+10 AS[10day],row+20 AS[20day],row+60 AS [60day], lishijiage.shou INTO TEMPB FROM dbo.lishijiage INNER JOIN dbo.VCalendar ON riqi=CDate
---- WHERE dbo.lishijiage.code='002610' 
-- ORDER BY riqi ASC
 
  DROP TABLE TEMPC

 go
 SET STATISTICS IO  ON 
 --龙虎榜营业部买入后股票5天后,10天后，20天后，60天后 价格
SELECT TOP 100 Percent dbo.TEMPA.*,TEMPB.shou AS [5dayshou],C.shou AS [10dayshou],D.shou AS [20dayshou],E.shou AS [60dayshou] INTO  TEMPC FROM TEMPA LEFT JOIN TEMPB ON dbo.TEMPA.[5day] = dbo.TEMPB.[row] AND TEMPA.code=TEMPB.code
LEFT JOIN dbo.TEMPB AS C ON dbo.TEMPA.[10day] = C.[row] AND TEMPA.code=C.code 
LEFT JOIN dbo.TEMPB AS D ON dbo.TEMPA.[20day] = D.[row] AND TEMPA.code=D.code 
LEFT JOIN dbo.TEMPB AS E ON dbo.TEMPA.[60day] = E.[row] AND TEMPA.code=E.code 
 --WHERE dbo.TEMPA.code='603019' 
  ORDER BY dbo.TEMPA.riqi desc


   DROP TABLE TEMPD

 go
 --龙虎榜营业部买入当天，做差5天后，做差10天后，做差20天后，做差60天后 价格结果
 SELECT shou-[5dayshou] AS [5dayjia],shou-[10dayshou] AS [10dayjia],shou-[20dayshou] AS  [20dayjia],shou-[60dayshou] AS  [60dayjia] ,TEMPC.* INTO TEMPD FROM dbo.TEMPC
 
  DROP TABLE TEMPE
 go
 --龙虎榜营业部买入当天，做差5天后，做差10天后，做差20天后，做差60天后 价格结果输出胜负标识字段
  SELECT  CASE WHEN  [5dayjia]>0 THEN 1  ELSE 0 END AS [5daysheng] ,
  CASE WHEN  [10dayjia]>0 THEN 1  ELSE 0 END AS [10daysheng] ,
  CASE WHEN  [20dayjia]>0 THEN 1  ELSE 0 END AS [20daysheng] ,
  CASE WHEN  [60dayjia]>0 THEN 1  ELSE 0 END AS [60daysheng] ,
  CASE WHEN  Trade LIKE 'B%' THEN 1  ELSE 0 END AS [maimai] ,
   TEMPD.*  
   INTO TEMPE 
    FROM TEMPD
   -- WHERE code='603019' 

 

    DROP TABLE TEMPFB
 go
 --龙虎榜营业部买入买后后续5天股价涨了 营业部出现的次数
   SELECT 
  COUNT(1) OVER(PARTITION BY Sales )  AS [5dayBsheng] ,
	 *
	 INTO TEMPFB 
	 FROM (SELECT DISTINCT * FROM    TEMPE) AS T
	 WHERE  maimai=1 AND [5daysheng]=1

	 DROP TABLE TEMPFS
	 go
	 --龙虎榜营业部卖出后后续5天股价跌了 营业部出现的次数
   SELECT 
  COUNT(1) OVER(PARTITION BY Sales )  AS [5daySsheng] ,
	 *
	 INTO TEMPFS 
	 FROM (SELECT DISTINCT * FROM    TEMPE) AS T
	 WHERE  maimai=0 AND [5daysheng]=0

	  DROP TABLE TEMPG
	 go
	 -- 龙虎榜营业部总买入出现的次数
	    SELECT 
  count(1) OVER(PARTITION BY Sales )  AS [chuxiancishu] ,
	 *
	 INTO TEMPG 
	 FROM (SELECT DISTINCT *   FROM   TEMPE) AS T

	  WHERE  maimai=1 
 
 


     DROP TABLE TEMPHB
	 go
	 --龙虎榜上营业部买入营业部相同名称
 SELECT DISTINCT TEMPFB.*,TEMPG.[chuxiancishu]  INTO  TEMPHB FROM TEMPFB inner JOIN TEMPG  ON TEMPFB.sales=TEMPG.sales
  --龙虎榜上营业部买入后5日胜率
 SELECT DISTINCT CAST(([5dayBsheng]*1.0/chuxiancishu) AS DECIMAL(18,2))*100, * FROM  TEMPHB


    
 --    DROP TABLE TEMPHS
	-- go
	-- --龙虎榜上营业部卖出相同
 --SELECT TEMPFS.*,TEMPG.[chuxiancishu]  INTO  TEMPHS FROM TEMPFS INNER JOIN TEMPG  ON TEMPFS.sales=TEMPG.sales
 --  --龙虎榜上营业部卖出后5日胜率
 --SELECT DISTINCT CAST(([5daySsheng]*1.0/chuxiancishu) AS DECIMAL(18,2))*100, * FROM TEMPHS

 

 

 


 
  

 
 



 