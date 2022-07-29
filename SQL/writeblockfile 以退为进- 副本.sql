 --查询排序
 --SELECT * FROM  dbo.T901
 --WHERE qiri='2022-04-13'
 --ORDER BY code desc


 
 
SELECT  DISTINCT   CONCAT('exec master.dbo.xp_cmdshell ''echo '+CONVERT(varchar(5), riqi, 10)+'                                             '+CONVERT(varchar(5), riqi, 10)+'                                                               >>C:\zd_xczq\T0002\blocknew\blocknew.cfg"''',';')
  ,  riqi,
   CONVERT(varchar(5), riqi, 10)
  FROM [stock].[dbo].[T902]
  ORDER BY riqi DESC   
 
 

SELECT  CONCAT('exec master.dbo.xp_cmdshell ''echo '+REPLACE(REPLACE(code,'sh.',1),'sz.',0)+'   >>C:\zd_xczq\T0002\blocknew\"'+ CONVERT(varchar(5), riqi, 10)+'                                            .blk"''',';')
  ,code,
  REPLACE(REPLACE(code,'sh.',1),'sz.',0),
  riqi,
   CONVERT(varchar(5), riqi, 10)
  FROM [stock].[dbo].[T902] 
  --WHERE qiri>'2022-04-01'
  ORDER BY riqi desc 
 