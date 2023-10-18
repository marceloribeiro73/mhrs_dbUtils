DECLARE @userNameSource NVARCHAR(128) = ''
DECLARE @userPassSource NVARCHAR(128) = ''
DECLARE @dbSchemaTableSource NVARCHAR(128) = ''
DECLARE @dbSchemaTableDestination NVARCHAR(128) = ''
DECLARE @delayWaitBetweenOperations CHAR(8) = '00:01:00'

DECLARE @isCmdShellEnable BIT = (SELECT CAST(value_in_use as INT) FROM sys.configurations where name = 'xp_cmdshell')
IF @isCmdShellEnable = 0 
BEGIN
    PRINT 'xp_cmdShell is disabled . BCP operations aren''t executed in this condition. '
END
ELSE
BEGIN
    -- Executing export bcp operations
    DECLARE @cmdBpcExec NVARCHAR(512), @idBpcCmd INT, @startTimeExec DATETIME, @endTimeExec DATETIME, @xp_cmdShellResult INT
    DECLARE exportOps CURSOR FAST_FORWARD FOR
        SELECT top 20 
            a.command,
            a.idCommand
        FROM dbo.tmp_data_migration_control a
        WHERE a.exporting = 1
            AND a.execStartDate IS NULL
            AND a.dbSchemaTableDestination = @dbSchemaTableDestination
            AND a.dbSchemaTableSource = @dbSchemaTableSource
        ORDER BY lowerBoundary ASC
    OPEN exportOps 
    FETCH NEXT FROM exportOps
    INTO @cmdBpcExec, @idBpcCmd
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT 'Executing the command: ' + @cmdBpcExec
        SET @cmdBpcExec = CONCAT(@cmdBpcExec, ' -U ',@userNameSource, ' -P ',@userPassSource)
        SET @startTimeExec = GETDATE()
        EXEC @xp_cmdShellResult = xp_cmdshell @cmdBpcExec;
        IF @xp_cmdShellResult = 0
        BEGIN
            SET @endTimeExec = GETDATE()
            UPDATE A SET A.execStartDate = @startTimeExec, a.execEndDate = @endTimeExec, a.statusLastExecIsError = @xp_cmdShellResult
            FROM dbo.tmp_data_migration_control A
            WHERE A.idCommand = @idBpcCmd
            PRINT 'Execution succeed'
        END
        ELSE 
        BEGIN
            UPDATE A SET A.execStartDate = @startTimeExec, a.statusLastExecIsError = @xp_cmdShellResult
            FROM dbo.tmp_data_migration_control A
            WHERE A.idCommand = @idBpcCmd
            PRINT 'Execution failed'
        END
		WAITFOR DELAY @delayWaitBetweenOperations
        FETCH NEXT FROM exportOps
        INTO @cmdBpcExec, @idBpcCmd
    END
    CLOSE exportOps
    DEALLOCATE exportOps
END