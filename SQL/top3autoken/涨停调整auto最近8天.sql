--SCS���4����ͣ���� ���8������ͣ �����һ�첻����ͣ ��������ھ��߸���
--����������ɼ��ڽ���֧��λ���ִ�������λ���ƣ��ڶ��������ǿ���ո��� ���ۿɼ�ǰ����һ�����������
 -----------------------------------------------------------------------------------
    
USE stock 
go 

--SELECT    ROW_NUMBER() OVER( PARTITION BY code ORDER BY riqi ASC) AS riqihao,*
--INTO T90
--FROM     dbo.lishijiager
--WHERE  riqi >='2023-06-19' and riqi <='2023-07-03'

DECLARE @i INT;
SET @i = (SELECT COUNT(1) FROM  dbo.T90 WHERE code = 'sz.000001')

--WHILE (@i > 5) 
	--BEGIN
	--SELECT @i  
		;WITH T AS (
				SELECT riqihao,pctChg AS zhangdie,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],[kai],[shou],[di],[gao],[chengjiaoliang],[pctChg] 
				FROM     dbo.T90   
				WHERE    riqihao >= @i-8 AND  riqihao<=@i  )
			,T2 AS (
				SELECT *
				FROM T 
				WHERE zhangdie>=9.94 AND riqihao!=@i
			)
			--SELECT * FROM T2
			,T5 AS (
		  		--����ͣ�� �����۸����������������� ��ͳ�ƺ��������ߵ�����
                SELECT COUNT(1) OVER (PARTITION BY T.code) AS zhangdiezhouqishu,T2.code, T2.code AS code1,T2.riqi AS kaishiriqi,T.riqi,
                       T.riqihao,T.shitifudu,T.zhangdie,T2.riqihao AS zhangtingjiariqihao,T.riqihao AS zuihouriqihao,
                       T.di,T.kai,T.shou,T.gao,MIN(T.shitifudu) OVER (PARTITION BY T.code) AS zuidadiefu
                FROM  T2 INNER JOIN T ON T2.code = T.code AND T2.riqihao < T.riqihao 
                )
				--SELECT * FROM T5 
			,T10 AS (
				--���Һ���K�� ���������һ���������ڲ�����ͣ���
                SELECT   *
                FROM     T5 
                 
				)

				---SELECT  * FROM  T10
				 SELECT DISTINCT '1900-01-01' AS zhangtingriqi,code,  '1900-01-01' AS ciriqi
				 				 				INTO T9090 
								FROM T10 
			 
					
      -- SET @i = @i - 1;	
	--END
