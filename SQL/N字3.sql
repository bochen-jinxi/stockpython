---N�ֽṹ ���ߵ���µ�4��֮�ڼ��ص� 2��N�ֽṹ
 -----------------------------------------------------------------------------------
 --�����8�������յ�K�߼��ߵ��
USE stock 
   go 
WITH    T AS ( SELECT   ( CASE WHEN ( shou - kai ) > 0 THEN 1
                               WHEN ( shou - kai ) = 0 THEN 0
                               WHEN ( shou - kai ) < 0 THEN -1
                          END ) AS zhangdie ,
						    ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi ASC ) AS riqihao2 ,
                        [code] ,
                        [riqi] ,
                        [kai] ,
                        [shou] ,
                        [di] ,
                        [gao] ,
                        [chengjiaoliang] ,
                        1 AS [pctChg]
               FROM     dbo.lishijiager
               WHERE    riqi >= '2021-10-01'
                        AND riqi <= '2021-11-10'
--	AND code like '%sh.600057%'
                        
             )-----------------------------------------------------------------
	,   T2
          AS (
		  --ȡ���ֵʱ��  ��Сֵʱ�� ʵ����� ��Ӱ�ߺ�ʵ��ı�ֵ ��Ӱ�߷���
               SELECT   ( CASE zhangdie
                            WHEN 1 THEN ( shou - kai ) / kai
                          END ) * 100 AS shitifudu ,
                        ( ( CASE zhangdie
                              WHEN 1 THEN ( gao - shou )
                            END ) / ( CASE zhangdie
                                        WHEN 1 THEN ( shou - kai )
                                      END ) ) AS syxbst ,
                        ( CASE zhangdie
                            WHEN 1 THEN ( gao - shou ) / shou
                          END ) * 100 AS syxfd ,
                      
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqi DESC ) AS riqihao ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY gao DESC ) AS RowID ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY di ASC ) AS RowID2 ,
                        *
               FROM     T
             ),
			 --��ߵ�����
        T3
          AS ( SELECT   *
               FROM     T2
               WHERE    RowID = 1
                        AND zhangdie = 1
                                             
             ),
		
			 --	 ���ߵ���µ�4��֮�ڼ��ص�
        T5
          AS ( SELECT   T2.riqihao2 - T3.riqihao2 AS num ,
                        T3.riqi AS griqi ,
                        T2.riqi AS d1riqi ,
                        T2.* ,
                        T2.di AS d1jiage
               FROM     T3
                        INNER JOIN T2 ON T3.code = T2.code
               WHERE    ( T2.riqihao2 - 1 = T3.riqihao2
                          OR T2.riqihao2 - 2 = T3.riqihao2
                          OR T2.riqihao2 - 3 = T3.riqihao2
                          OR T2.riqihao2 - 4 = T3.riqihao2
                        )
                        AND T2.zhangdie = -1
             ),
		
			 --1�͵����� ���ڵ����ź�
        T6
          AS ( SELECT   MAX(num) OVER ( PARTITION BY code ) AS jin4tianxiadieshu ,
                        MIN(d1jiage) OVER ( PARTITION BY code ) AS d1jia ,
                        ROW_NUMBER() OVER ( PARTITION BY code ORDER BY riqihao2 DESC ) AS num3 ,
                        *
               FROM     T5
             )
			  -- 1�͵��������
        ,T7
          AS ( SELECT   *
               FROM     T6
               WHERE    num3 = 1
             )
			 ,T8 AS (
			 	 	 --��ͼ۴���4������ͼ� �����ں������ڵ�
      SELECT T7.griqi,T7.d1riqi,T2.code, T2.riqi AS lang2riqi,T2.shou,
	   T2.riqihao2 AS lang2riqihao2,T2.di AS lang2djia, T7.jin4tianxiadieshu,T7.d1jia,(MAX(T2.riqihao2) OVER(PARTITION BY T2.code) -T7.riqihao2)  AS chariqihao2,  count(T2.riqihao2) OVER(PARTITION BY  T2.code) AS countriqihao2           FROM     T2 INNER JOIN  T7 ON T2.code = T7.code
               WHERE        T2.di>d1jia AND  T2.riqihao2>T7.riqihao2
           
			)
			,T9 AS (
			SELECT ROW_NUMBER() OVER(PARTITION BY code ORDER BY lang2riqihao2 desc ) AS num4, * FROM T8 WHERE chariqihao2=countriqihao2
			)
			SELECT * FROM T9 WHERE num4=1
			  --2�͵���1�͵� 1�׵� �� 2�׵�֮��͵���3����֮��
                 AND      ( ( lang2djia - d1jia ) / d1jia ) * 100 <3
				 AND lang2riqi='2021-11-10'
				 ORDER BY shou DESC 


 


	
		 
  