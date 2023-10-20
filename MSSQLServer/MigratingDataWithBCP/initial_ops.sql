USE [DbExemple]


DECLARE @maxRowsPerFile AS bigint = 16485762

-- Source parameters
DECLARE @nameTableSource NVARCHAR(128) = ''
DECLARE @columnKeySource NVARCHAR(128) = ''
DECLARE @serverSource NVARCHAR(128) = ''
DECLARE @filePathSource NVARCHAR (128) = ''

-- Destination parameters
DECLARE @nameTableDestination NVARCHAR(128) = ''
DECLARE @serverDestination NVARCHAR(128) = ''
DECLARE @batchSize INT = 10240

DECLARE @bcpCommandOut NVARCHAR(512)
DECLARE @bcpCommandIn NVARCHAR(512) 
DECLARE @lowerbound AS bigint 
DECLARE @upperbound AS bigint 

--Load and Generate commands 
IF OBJECT_ID(N'dbo.tmp_data_migration_control') IS NULL
    CREATE TABLE dbo.tmp_data_migration_control (
    idCommand int IDENTITY(1,1),
    dateGenerated DATETIME,
    dbSchemaTableSource NVARCHAR(512),
    columnKeySource NVARCHAR(128),
    dbSchemaTableDestination NVARCHAR(512),
    lowerBoundary bigint,
    upperBoundary bigint,
    command NVARCHAR(512),
    exporting BIT,
    execStartDate DATETIME,
    execEndDate DATETIME,
    statusLastExecIsError BIT
    )


CREATE TABLE #temp_boundaries (colunmKey bigint)
DECLARE @SQL_CMD NVARCHAR(512)= 'INSERT INTO #temp_boundaries SELECT colunmKey FROM ( SELECT '+ @columnKeySource +' AS colunmKey , ROW_NUMBER() OVER(ORDER BY '+@columnKeySource+') AS the_row_number FROM '+@nameTableSource+') AS t  WHERE the_row_number % '+ cast(@maxRowsPerFile as nvarchar)+' = 0'
EXEC (@SQL_CMD)

-- Load Lowerbond
SET @lowerbound = (SELECT MIN(colunmKey)-1 FROM #temp_boundaries)

DECLARE boundaries CURSOR FOR
    SELECT colunmKey
    FROM #temp_boundaries

OPEN boundaries


FETCH NEXT FROM boundaries
INTO @upperbound

IF @lowerbound = @upperbound
BEGIN
    SET @bcpCommandOut  = 'bcp "SELECT * FROM '+@nameTableSource +'" queryout "'+@filePathSource+'\'+@nameTableSource+'_file.bcp" -w -S '+@serverSource
    INSERT INTO dbo.tmp_data_migration_control (dateGenerated,dbSchemaTableSource,columnKeySource,dbSchemaTableDestination,
        lowerBoundary,upperBoundary,command,exporting,execStartDate,execEndDate)
        VALUES ( 
            GETDATE() 
            , @nameTableSource 
            , @columnKeySource 
            , @nameTableDestination 
            , @lowerbound 
            , @upperbound 
            , @bcpCommandOut 
            , 1 
            , NULL 
            , NULL )

   SET @bcpCommandIn  = 'bcp '+@nameTableDestination+' IN "'+@filePathSource+'\'+@nameTableSource+'_file.bcp" -b'+cast(@batchSize as varchar)+' -w -S '+@serverSource 
    INSERT INTO dbo.tmp_data_migration_control (dateGenerated,dbSchemaTableSource,columnKeySource,dbSchemaTableDestination,
        lowerBoundary,upperBoundary,command,exporting,execStartDate,execEndDate)
	VALUES(
			 GETDATE() 
            , @nameTableSource 
            , @columnKeySource 
            , @nameTableDestination  
            , @lowerbound 
            , @upperbound 
            , @bcpCommandIn 
            , 0 
            , NULL 
            , NULL )

END
ELSE

    DECLARE @filecount AS int = 1
    BEGIN
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @bcpCommandOut = 'bcp "SELECT * FROM '+ @nametablesource +' WHERE '+@columnKeySource+' > ' + CAST(@lowerbound AS varchar) + ' AND '+@columnKeySource+' <= ' + CAST(@upperbound AS varchar) + '" queryout "'+ @filepathsource +'\'+ @nametablesource +'_file_' + CAST(@filecount AS varchar) + '.bcp" -w -S '+@serversource
            INSERT INTO dbo.tmp_data_migration_control (dateGenerated,dbSchemaTableSource,columnKeySource,dbSchemaTableDestination,
            lowerBoundary,upperBoundary,command,exporting,execStartDate,execEndDate)
            VALUES ( 
            GETDATE() 
            , @nameTableSource 
            , @columnKeySource 
            , @nameTableDestination 
            , @lowerbound 
            , @upperbound 
            , @bcpCommandOut 
            , 1 
            , NULL 
            , NULL )

            SET @bcpCommandIn = 'bcp '+@nameTableDestination+' IN "'+@filePathSource+'\'+@nameTableSource+'_file_'+ CAST(@filecount AS varchar)+'.bcp" -b'+cast(@batchSize as varchar)+' -w -S '+@serverSource
            INSERT INTO dbo.tmp_data_migration_control (dateGenerated,dbSchemaTableSource,columnKeySource,dbSchemaTableDestination,
                lowerBoundary,upperBoundary,command,exporting,execStartDate,execEndDate)
	        VALUES(
                GETDATE() 
                , @nameTableSource 
                , @columnKeySource 
                , @nameTableDestination  
                , @lowerbound 
                , @upperbound 
                , @bcpCommandIn 
                , 0 
                , NULL 
                , NULL )
            
            SET @filecount = @filecount + 1
            SET @lowerbound = @upperbound
            FETCH NEXT FROM boundaries
            INTO @upperbound
        END
        SET @bcpCommandOut = 'bcp "SELECT * FROM '+ @nametablesource +' WHERE '+@columnKeySource+' > ' + CAST(@lowerbound AS varchar) + '" queryout "'+ @filepathsource +'\'+ @nametablesource +'_file_' + CAST(@filecount AS varchar) + '.bcp" -w -S '+@serversource
        INSERT INTO dbo.tmp_data_migration_control (dateGenerated,dbSchemaTableSource,columnKeySource,dbSchemaTableDestination,
        lowerBoundary,upperBoundary,command,exporting,execStartDate,execEndDate)
        VALUES ( 
            GETDATE() 
            , @nameTableSource 
            , @columnKeySource 
            , @nameTableDestination 
            , @lowerbound 
            , @upperbound 
            , @bcpCommandOut 
            , 1 
            , NULL 
            , NULL )
        SET @bcpCommandIn = 'bcp '+@nameTableDestination+' IN "'+@filePathSource+'\'+@nameTableSource+'_file_'+ CAST(@filecount AS varchar)+'.bcp" -b'+cast(@batchSize as varchar)+' -w -S '+@serverSource
        INSERT INTO dbo.tmp_data_migration_control (dateGenerated,dbSchemaTableSource,columnKeySource,dbSchemaTableDestination,
            lowerBoundary,upperBoundary,command,exporting,execStartDate,execEndDate)
	    VALUES(
		    GETDATE() 
          , @nameTableSource 
          , @columnKeySource 
          , @nameTableDestination  
          , @lowerbound 
          , @upperbound 
          , @bcpCommandIn 
          , 0 
          , NULL 
          , NULL )

    END
CLOSE boundaries
DEALLOCATE boundaries