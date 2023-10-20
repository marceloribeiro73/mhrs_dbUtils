CREATE OR ALTER PROCEDURE P_IMPORTING_OPS 
@p_user_name_destination NVARCHAR(128)
,@p_use_passwd_destination NVARCHAR(128)
,@p_db_schema_table_souce NVARCHAR(128)
,@p_db_schema_table_destination NVARCHAR(128)
,@p_delay_between_operations CHAR(8) = '00:01:00'
,@p_qtd_operations_per_exec INT 
AS
BEGIN
    --Check if xp_cmdShell is enable, if not, bpc operations aren't going to be executed 
    DECLARE @is_xp_cmdshell_enable BIT = (SELECT CAST(value_in_use as BIT) FROM sys.configurations sc WHERE sc.name = 'xp_cmdshell')
    IF @is_xp_cmdshell_enable = 0
    BEGIN
        PRINT 'xp_cmdShell is disabled. BCP operations aren''t goind to be executed in this condition'
    END
    ELSE
    BEGIN
        DECLARE @cmd_bcp_exec_imp NVARCHAR(MAX), @id_bcp_cmd_imp INT, @start_time_exec_imp DATETIME, @end_time_exec DATETIME, @xp_cmdshell_result_imp INT
        DECLARE cur_import_ops CURSOR FAST_FORWARD FOR
            WITH exeported 
            AS
            (
                SELECT
                    tdmc.lowerBoundary
                    ,tdmc.dbSchemaTableSource
                    ,tdmc.dbSchemaTableDestination
                FROM dbo.tmp_data_migration_control tdmc
                WHERE tdmc.execEndDate IS NOT NULL
                    AND tdmc.exporting = 1 
                    AND tdmc.statusLastExecIsError = 0
                    AND tdmc.dbSchemaTableSource = @p_db_schema_table_souce
                    AND tdmc.dbSchemaTableDestination = @p_db_schema_table_destination
            )
            SELECT TOP (@p_qtd_operations_per_exec)
                tdmc.idCommand
                ,tdmc.command
            FROM dbo.tmp_data_migration_control tdmc
            JOIN exported exp ON tdmc.lowerBoundary = exp.lowerBoundary
                AND tdmc.dbSchemaTableDestination = exp.dbSchemaTableDestination
                AND tdmc.dbSchemaTableSource = exp.dbSchemaTableSource
            WHERE tdmc.exporting = 0
                AND tdmc.execStartDate IS NULL
            ORDER BY tdmc.idCommand ASC
        OPEN cur_import_ops
        FETCH NEXT FROM cur_import_ops
            INTO @id_bcp_cmd_imp, @cmd_bcp_exec_imp
        WHILE @@FETCH_STATUS = 0
        BEGIN
            PRINT 'Executing the command: ' + @cmd_bcp_exec_imp
            SET @cmd_bcp_exec_imp = CONCAT(@cmd_bcp_exec_imp, ' -E -U ', @p_user_name_destination, ' -P ', @p_use_passwd_destination)
            SET @start_time_exec = GETDATE()
            EXEC @xp_cmdshell_result_imp = xp_cmdshell @cmd_bcp_exec_imp
            IF @xp_cmdshell_result_imp = 0
            BEGIN
                SET @end_time_exec = GETDATE()
                UPDATE tdmc SET tdmc.execStartDate = @start_time_exec, tdmc.execEndDate =@end_time_exec, tdmc.statusLastExecIsError = @xp_cmdshell_result_imp
                FROM dbo.tmp_data_migration_control tdmc
                WHERE tdmc.idCommand = @id_bcp_cmd_imp
                PRINT 'Execution succeed'
            END
            ELSE
            BEGIN
                UPDATE tdmc SET tdmc.execStartDate = @start_time_exec_imp, tdmc.statusLastExecIsError = @xp_cmdshell_result_imp
                FROM dbo.tmp_data_migration_control tdmc
                WHERE tdmc.idCommand = @id_bcp_cmd_imp
                PRINT 'Execution failed'
            END
            WAITFOR DELAY @p_delay_between_operations
            FETCH NEXT FROM cur_import_ops
                INTO @id_bcp_cmd_imp, @cmd_bcp_exec_imp
        END
        CLOSE cur_import_ops
        DEALLOCATE cur_import_ops
    END
END
