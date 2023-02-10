  --²éÑ¯ÅÅÐò
 --SELECT * FROM  dbo.T900
 --WHERE ciriqi='2022-04-13'
 --ORDER BY code desc

 
 
SELECT  DISTINCT   CONCAT('exec master.dbo.xp_cmdshell ''echo '+CONVERT(varchar(5), ciriqi, 10)+'-'+CONVERT(varchar(5), zhangtingriqi, 10)+'                                       '+CONVERT(varchar(5), ciriqi, 10)+'-'+CONVERT(varchar(5), zhangtingriqi, 10)+'                                                         >>C:\zd_zsone\T0002\blocknew\blocknew.cfg"''',';')
  ,  ciriqi,
   CONVERT(varchar(5), ciriqi, 10)
  FROM [stock].[dbo].[T9000]
  ORDER BY ciriqi DESC   
   
SELECT  CONCAT('exec master.dbo.xp_cmdshell ''echo '+REPLACE(REPLACE(code,'sh.',1),'sz.',0)+'   >>C:\zd_zsone\T0002\blocknew\"'+ CONVERT(varchar(5), ciriqi, 10)+'-'+ CONVERT(varchar(5), zhangtingriqi, 10)+'                                      .blk"''',';')
  ,code,
  REPLACE(REPLACE(code,'sh.',1),'sz.',0),
  ciriqi,
   CONVERT(varchar(5), ciriqi, 10)
  FROM [stock].[dbo].[T9000] 
  ORDER BY ciriqi desc 