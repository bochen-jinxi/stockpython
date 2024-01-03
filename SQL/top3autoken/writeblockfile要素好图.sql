  --查询排序
 --SELECT * FROM  dbo.T900
 --WHERE ciriqi='2022-04-13'
 --ORDER BY code desc

 
 
SELECT  DISTINCT   CONCAT('exec master.dbo.xp_cmdshell ''echo '+CONVERT(varchar(5), kaishiriqi, 10)+'-'+CONVERT(varchar(5), kaishiriqi, 10)+'                                       '+CONVERT(varchar(5), kaishiriqi, 10)+'-'+CONVERT(varchar(5), kaishiriqi, 10)+'                                                         >>C:\zd_zsone\T0002\blocknew\blocknew.cfg"''',';')
  ,  kaishiriqi,
   CONVERT(varchar(5), kaishiriqi, 10)
  FROM [stock].[dbo].[T10000]
  ORDER BY kaishiriqi DESC   
   
SELECT  CONCAT('exec master.dbo.xp_cmdshell ''echo '+REPLACE(REPLACE(code,'sh.',1),'sz.',0)+'   >>C:\zd_zsone\T0002\blocknew\"'+ CONVERT(varchar(5), kaishiriqi, 10)+'-'+ CONVERT(varchar(5), kaishiriqi, 10)+'                                      .blk"''',';')
  ,code,
  REPLACE(REPLACE(code,'sh.',1),'sz.',0),
  kaishiriqi,
   CONVERT(varchar(5), kaishiriqi, 10)
  FROM [stock].[dbo].[T10000] 
  ORDER BY kaishiriqi desc 