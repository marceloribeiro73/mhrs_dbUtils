DECLARE @isCmdShellEnable BIT = (SELECT CAST(value_in_use as INT) FROM sys.configurations where name = 'xp_cmdshell')
DECLARE @userNameDestination NVARCHAR(128) = ''
DECLARE @userPassDestination NVARCHAR(128) = ''
DECLARE @dbSchemaTableSource NVARCHAR(128) = ''
DECLARE @dbSchemaTableDestination NVARCHAR(128) = ''
DECLARE @delayWaitBetweenOperations CHAR(8) = '00:01:00'

IF @isCmdShellEnable = 0 
BEGIN
    PRINT 'xp_cmdShell is disabled . BCP operations aren''t executed in this condition. '
END
ELSE
BEGIN
    -- Executing import bcp operations
    DECLARE @cmdBpcExecImp NVARCHAR(512), @idBpcCmdImp INT, @startTimeExecImp DATETIME, @endTimeExecImp DATETIME, @xp_cmdShellResultImp INT
    DECLARE importOps CURSOR FAST_FORWARD FOR 
        WITH exported
        AS
        (
            SELECT  a.lowerBoundary, a.dbSchemaTableSource, a.dbSchemaTableDestination
            FROM dbo.tmp_data_migration_control a
            WHERE a.exporting = 1
                AND a.execEndDate IS NOT NULL
                AND a.statusLastExecIsError = 0
                AND a.dbSchemaTableSource = @dbSchemaTableSource
                AND a.dbSchemaTableDestination = @dbSchemaTableDestination
        )
        SELECT top 20
            a.idCommand,
            a.command
        FROM dbo.tmp_data_migration_control a
        JOIN exported b
            ON a.lowerBoundary = b.lowerBoundary 
                AND a.dbSchemaTableSource = b.dbSchemaTableSource
                AND a.dbSchemaTableDestination = b.dbSchemaTableDestination
        WHERE a.exporting  = 0
            AND a.execStartDate IS NULL
        ORDER BY a.idCommand ASC
    OPEN importOps
    FETCH NEXT FROM importOps 
    INTO @idBpcCmdImp, @cmdBpcExecImp
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT 'Executing the command: ' + @cmdBpcExecImp
        SET @cmdBpcExecImp = CONCAT(@cmdBpcExecImp, ' -E -U ',@userNameDestination, ' -P ',@userPassDestination)
        SET @startTimeExecImp = GETDATE()
        EXEC @xp_cmdShellResultImp = xp_cmdshell @cmdBpcExecImp;
        IF @xp_cmdShellResultImp = 0
        BEGIN
            SET @endTimeExecImp = GETDATE()
            UPDATE A SET A.execStartDate = @startTimeExecImp, a.execEndDate = @endTimeExecImp, a.statusLastExecIsError = @xp_cmdShellResultImp
            FROM dbo.tmp_data_migration_control A
            WHERE A.idCommand = @idBpcCmdImp
            PRINT 'Execution succeed'
        END
        ELSE 
        BEGIN
            UPDATE A SET A.execStartDate = @startTimeExecImp, a.statusLastExecIsError = @xp_cmdShellResultImp
            FROM dbo.tmp_data_migration_control A
            WHERE A.idCommand = @idBpcCmdImp
            PRINT 'Execution failed'
        END
		WAITFOR DELAY @delayWaitBetweenOperations
        FETCH NEXT FROM importOps 
        INTO @idBpcCmdImp, @cmdBpcExecImp
    END
    CLOSE importOps
    DEALLOCATE importOps
END