  --查询排序
 --SELECT * FROM  dbo.T900
 --WHERE zhuriqi='2022-04-13'
 --ORDER BY code desc


 
SELECT  DISTINCT   CONCAT('exec master.dbo.xp_cmdshell ''echo '+CONVERT(varchar(5), zhuriqi, 10)+'-'+CONVERT(varchar(5), riqi, 10)+'                                       '+CONVERT(varchar(5), zhuriqi, 10)+'-'+CONVERT(varchar(5), riqi, 10)+'                                                         >>C:\zd_zsone\T0002\blocknew\blocknew.cfg"''',';')
  ,  zhuriqi,
   CONVERT(varchar(5), zhuriqi, 10)
  FROM [stock].[dbo].[T900]
  ORDER BY zhuriqi DESC   
   
--exec master.dbo.xp_cmdshell 'echo 03-07                                             03-07                                                               >>C:\zd_zsone\T0002\blocknew\blocknew.cfg"';
--exec master.dbo.xp_cmdshell 'echo 03-08                                             03-08                                                               >>C:\zd_zsone\T0002\blocknew\blocknew.cfg"';
--exec master.dbo.xp_cmdshell 'echo 03-09                                             03-09                                                               >>C:\zd_zsone\T0002\blocknew\blocknew.cfg"';
--exec master.dbo.xp_cmdshell 'echo 03-10                                             03-10                                                               >>C:\zd_zsone\T0002\blocknew\blocknew.cfg"';


SELECT  CONCAT('exec master.dbo.xp_cmdshell ''echo '+REPLACE(REPLACE(code,'sh.',1),'sz.',0)+'   >>C:\zd_zsone\T0002\blocknew\"'+ CONVERT(varchar(5), zhuriqi, 10)+'-'+ CONVERT(varchar(5), riqi, 10)+'                                      .blk"''',';')
  ,code,
  REPLACE(REPLACE(code,'sh.',1),'sz.',0),
  zhuriqi,
   CONVERT(varchar(5), zhuriqi, 10)
  FROM [stock].[dbo].[T900] 
  --WHERE zhuriqi>'2022-04-23'
  ORDER BY zhuriqi desc 

-- exec master.dbo.xp_cmdshell 'echo 1688508   >>C:\zd_zsone\T0002\blocknew\"03-10                                            .blk"';
--exec master.dbo.xp_cmdshell 'echo 1600319   >>C:\zd_zsone\T0002\blocknew\"03-09                                            .blk"';
--exec master.dbo.xp_cmdshell 'echo 1603133   >>C:\zd_zsone\T0002\blocknew\"03-08                                            .blk"';
--exec master.dbo.xp_cmdshell 'echo 0000633   >>C:\zd_zsone\T0002\blocknew\"03-08                                            .blk"';
--exec master.dbo.xp_cmdshell 'echo 0300089   >>C:\zd_zsone\T0002\blocknew\"03-08                                            .blk"';
--exec master.dbo.xp_cmdshell 'echo 1601857   >>C:\zd_zsone\T0002\blocknew\"03-07                                            .blk"';
 
