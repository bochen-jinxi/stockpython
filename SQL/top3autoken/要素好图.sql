--4要素好图
--买点描述： 好图
 -----------------------------------------------------------------------------------
    
USE stock 
go 

--SELECT    ROW_NUMBER() OVER( PARTITION BY code ORDER BY riqi Desc) AS riqihao,*
--INTO T90
--FROM     dbo.lishijiager
--WHERE code='sz.002920' and  riqi >='2022-12-21' and riqi <='2023-01-20'
 
		;WITH T AS (
				SELECT riqihao,pctChg AS zhangdie,(shou - kai)/kai*100 AS shitifudu,[code],[riqi],[kai],[shou],[di],[gao],[chengjiaoliang],[pctChg],
				IIF(shou>=kai,shou,kai)as maxval,IIF(shou<=kai,shou,kai)as minval				
				FROM     dbo.T90
				WHERE    riqihao <= 8)
			--	SELECT * FROM T	
			,T3 AS ( 	 
				SELECT ROW_NUMBER() OVER (PARTITION BY code ORDER BY shitifudu DESC) AS RowID,*,(gao/maxVal-1)*100 AS shangyingxiafudu,(minval/di-1) *100 AS xiayingxianfudu
				FROM T)
				--SELECT * FROM T3
			,T4 AS ( 
				-- 各代码最大实体的日期 价格
                SELECT   *
                FROM     T3
                WHERE    RowID = 1 AND riqihao>=5 )

				--SELECT * FROM T4
			,T5 AS (
		  		--见最大实体后 后续价格数据中所有阴阳线 并统计后续阴阳线的数量
                SELECT COUNT(1) OVER (PARTITION BY T3.code) AS zhangdiezhouqishu,T4.code,T4.riqi AS kaishiriqi,	T4.zhangdie AS zuidazhangshiti,	T3.riqi,
                       T3.riqihao,T3.shitifudu,T3.zhangdie,T4.riqihao AS zuigaojiariqihao,T3.riqihao AS zuihouriqihao,
                       T3.di,T3.kai,T3.shou,T3.gao,MIN(T3.shitifudu) OVER (PARTITION BY T3.code) AS zuidadieshiti,
					   T3.shangyingxiafudu,T3.xiayingxianfudu
                FROM  T4 INNER JOIN T3 ON T4.code = T3.code AND T4.riqihao > T3.riqihao
                WHERE    T4.RowID = 1)
				--SELECT * FROM T5  
				,T6 AS (
				SELECT COUNT(1) OVER (PARTITION BY T5.code) AS shangxiayingshu,  * FROM T5 
				--WHERE shangyingxiafudu<=1.5 AND xiayingxianfudu<=1.5 
				)
				INSERT INTO dbo.T10000(  [shangxiayingshu] ,  [zhangdiezhouqishu],  [code],  [kaishiriqi] ,  [zuidazhangshiti] ,  [riqi] ,
  [riqihao]   ,  [shitifudu] ,  [zhangdie] ,  [zuigaojiariqihao],  [zuihouriqihao] ,
  [di] ,  [kai],  [shou] ,  [gao] ,  [zuidadieshiti] ,  [shangyingxiafudu],
  [xiayingxianfudu])
            SELECT  *
			--INTO  T10000
				 FROM T6 
			--	WHERE code='sz.300151'
				ORDER BY shangyingxiafudu DESC  
			
			 
