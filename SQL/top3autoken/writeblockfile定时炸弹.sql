 --查询排序
 --SELECT * FROM  dbo.T901
 --WHERE qiri='2022-04-13'
 --ORDER BY code desc


 
 
SELECT  DISTINCT   CONCAT('exec master.dbo.xp_cmdshell ''echo '+CONVERT(varchar(5), zhariqi, 10)+'                                             '+CONVERT(varchar(5), zhariqi, 10)+'                                                               >>C:\zd_xczq\T0002\blocknew\blocknew.cfg"''',';')
  ,  zhariqi,
   CONVERT(varchar(5), zhariqi, 10)
  FROM [stock].[dbo].[T904]
  ORDER BY zhariqi DESC   
 
 

SELECT  CONCAT('exec master.dbo.xp_cmdshell ''echo '+REPLACE(REPLACE(code,'sh.',1),'sz.',0)+'   >>C:\zd_xczq\T0002\blocknew\"'+ CONVERT(varchar(5), zhariqi, 10)+'                                            .blk"''',';')
  ,code,
  REPLACE(REPLACE(code,'sh.',1),'sz.',0),
  zhariqi,
   CONVERT(varchar(5), zhariqi, 10)
  FROM [stock].[dbo].[T904] 
  --WHERE qiri>'2022-04-01'
  ORDER BY zhariqi desc 
 