  --查询排序
 --SELECT * FROM  dbo.T900
 --WHERE zhuriqi='2022-04-13'
 --ORDER BY code desc

 
 
SELECT  DISTINCT   CONCAT('exec master.dbo.xp_cmdshell ''echo '+CONVERT(varchar(5), zhuriqi, 10)+'-'+CONVERT(varchar(5), riqi, 10)+'                                       '+CONVERT(varchar(5), zhuriqi, 10)+'-'+CONVERT(varchar(5), riqi, 10)+'                                                         >>C:\zd_zsone\T0002\blocknew\blocknew.cfg"''',';')
  ,  zhuriqi,
   CONVERT(varchar(5), zhuriqi, 10)
  FROM [stock].[dbo].[T900]
  ORDER BY zhuriqi DESC   
   
SELECT  CONCAT('exec master.dbo.xp_cmdshell ''echo '+REPLACE(REPLACE(code,'sh.',1),'sz.',0)+'   >>C:\zd_zsone\T0002\blocknew\"'+ CONVERT(varchar(5), zhuriqi, 10)+'-'+ CONVERT(varchar(5), riqi, 10)+'                                      .blk"''',';')
  ,code,
  REPLACE(REPLACE(code,'sh.',1),'sz.',0),
  zhuriqi,
   CONVERT(varchar(5), zhuriqi, 10)
  FROM [stock].[dbo].[T900] 
  ORDER BY zhuriqi desc 