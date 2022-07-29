 --查询排序
 --SELECT * FROM  dbo.T901
 --WHERE tingriqi='2022-04-13'
 --ORDER BY code desc


 
SELECT  DISTINCT   CONCAT('exec master.dbo.xp_cmdshell ''echo '+CONVERT(varchar(5), tingriqi, 10)+'                                             '+CONVERT(varchar(5), tingriqi, 10)+'                                                               >>C:\zd_xczq\T0002\blocknew\blocknew.cfg"''',';')
  ,  tingriqi,
   CONVERT(varchar(5), tingriqi, 10)
  FROM [stock].[dbo].[T901]
  ORDER BY tingriqi DESC   
 
 

SELECT  CONCAT('exec master.dbo.xp_cmdshell ''echo '+REPLACE(REPLACE(code,'sh.',1),'sz.',0)+'   >>C:\zd_xczq\T0002\blocknew\"'+ CONVERT(varchar(5), tingriqi, 10)+'                                            .blk"''',';')
  ,code,
  REPLACE(REPLACE(code,'sh.',1),'sz.',0),
  tingriqi,
   CONVERT(varchar(5), tingriqi, 10)
  FROM [stock].[dbo].[T901] 
  --WHERE tingriqi>'2022-04-01'
  ORDER BY tingriqi desc 
 