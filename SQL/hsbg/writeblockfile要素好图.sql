  --查询排序
 --SELECT * FROM  dbo.T900
 --WHERE ciriqi='2022-04-13'
 --ORDER BY code desc

 
 
SELECT  DISTINCT   CONCAT('exec master.dbo.xp_cmdshell ''echo '+RIGHT(CONVERT(varchar(6),NAME),3)+RIGHT(CONVERT(varchar(5), kaishiriqi, 10),2)+'-'+CONVERT(varchar(5), jieshuriqi, 10)+'                                       '+RIGHT(CONVERT(varchar(6),NAME),3)+RIGHT(CONVERT(varchar(5), kaishiriqi, 10),2)+'-'+CONVERT(varchar(5), jieshuriqi, 10)+'                                                         >>C:\zd_zsone\T0002\blocknew\blocknew.cfg"''',';')
,  kaishiriqi,
CONVERT(varchar(5), kaishiriqi, 10)
FROM [stock].[dbo].[T10000]
ORDER BY kaishiriqi DESC   
   

 

 
   
  SELECT  DISTINCT   CONCAT('exec master.dbo.xp_cmdshell ''echo '+IIF(LEN(code)=5,'71#'+code,IIF(LEFT(code, 1) IN( '6','9'), '1'+code, '0'+code)) +'   >>C:\zd_zsone\T0002\blocknew\"'+ RIGHT(CONVERT(varchar(6),NAME),3)+RIGHT(CONVERT(varchar(5), kaishiriqi, 10),2)+'-'+ CONVERT(varchar(5), jieshuriqi, 10)+'                                      .blk"''',';')
  ,code,
 IIF(LEN(code)=5,'71#'+code,IIF(LEFT(code, 1) IN( '6','9'), '1'+code, '0'+code))
  kaishiriqi,
   CONVERT(varchar(5), kaishiriqi, 10)
  FROM [stock].[dbo].[T10000] 
  ORDER BY kaishiriqi desc 
-- OFFSET (1-1)*100 ROWS FETCH NEXT 100 ROWS ONLY;