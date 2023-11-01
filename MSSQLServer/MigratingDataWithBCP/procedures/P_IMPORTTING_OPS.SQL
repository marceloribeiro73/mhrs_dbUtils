CREATE OR ALTER PROCEDURE P_IMPORTING_OPS 
@p_user_name_destination NVARCHAR(128)
,@p_use_passwd_destination NVARCHAR(128)
,@p_db_schema_table_souce NVARCHAR(128)
,@p_db_schema_table_destination NVARCHAR(128)
,@p_delay_between_operations CHAR(8) = '00:01:00'
,@p_qtd_operations_per_exec INT 
AS
BEGIN
    -- add sources for parameters 
    DECLARE @cod_error BIT = 0 -- 0 is non error
    DECLARE @msg_error NVARCHAR(64) = 'ERROR: '
   
    --source if xp_cmdshell is enable, if isn't, anything is executed
    IF (SELECT CAST(value_in_use as INT) FROM sys.configurations sc WHERE sc.name = 'xp_cmdshell') = 0
    BEGIN
        SET @cod_error = 1
        PRINT @msg_error + 'xp_cmdShell is disabled . BCP operations aren''t executed in this condition.'
    END
    IF @p_qtd_operations_per_exec IS NULL or @p_qtd_operations_per_exec <= 0
    BEGIN
        SET @cod_error = 1
        PRINT @msg_error + 'Parameter @p_qtd_operations_per_exec is null or is under or equal a zero'
    END
    IF @p_delay_between_operations IS NULL
    BEGIN
        SET @cod_error = 1
        PRINT @msg_error + 'Parameter @p_delay_between_operations is null'
    END
    IF @p_user_name_destination IS NULL
    BEGIN
        SET @cod_error =1
        PRINT @msg_error + 'Parameter @p_user_name_destination is null'
    END
    ELSE IF (SELECT principal_id FROM sys.server_principals WHERE [name] = @p_user_name_destination) IS NULL
    BEGIN
        SET @cod_error = 1
        PRINT @msg_error + 'Login not exists'
    END
    ELSE IF (SELECT is_disabled FROM sys.server_principals WHERE [name] = @p_user_name_destination) = 1
    BEGIN
        SET @cod_error = 1
        PRINT @msg_error + 'Login is disabled'
    END
    IF @p_use_passwd_destination IS NULL
    BEGIN
        SET @cod_error = 1
        PRINT @msg_error + 'Parameter @p_use_passwd_destination is null'
    END
    IF @p_db_schema_table_souce IS NULL
    BEGIN
        SET @cod_error = 1
        PRINT @msg_error + 'Parameter @p_db_schema_table_souce is null'
    END
    ELSE
    BEGIN
        BEGIN TRY
            DECLARE @validate_cmd NVARCHAR(512) = 'SELECT TOP 1 * FROM ' + @p_db_schema_table_souce
            EXEC (@validate_cmd)
        END TRY
        BEGIN CATCH
            SET @cod_error = 1
            PRINT @msg_error + '@p_db_schema_table_souce is a invalid object'
        END CATCH
    END
    IF @p_db_schema_table_destination IS NULL
    BEGIN
        SET @cod_error = 1
        PRINT @msg_error + 'Parameter @p_db_schema_table_destination is null'
    END
    ELSE
    BEGIN
        BEGIN TRY
            DECLARE @validate_cmd_d NVARCHAR(512) = 'SELECT TOP 1 * FROM ' + @p_db_schema_table_destination
            EXEC (@validate_cmd_d)
        END TRY
        BEGIN CATCH
            SET @cod_error = 1
            PRINT @msg_error + '@p_db_schema_table_destination is a invalid object'
        END CATCH
    END
    IF @cod_error > 0
    BEGIN
        PRINT 'There are errors on pre-validation, please give a look on previous messages '-- Room from improvement is know
    END
    ELSE
    BEGIN
        DECLARE @cmd_bcp_exec_imp NVARCHAR(1024), @id_bcp_cmd_imp INT, @start_time_exec_imp DATETIME, @end_time_exec DATETIME, @xp_cmdshell_result_imp INT
        DECLARE cur_import_ops CURSOR FAST_FORWARD FOR
            WITH exported 
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
            SET @start_time_exec_imp = GETDATE()
            EXEC @xp_cmdshell_result_imp = xp_cmdshell @cmd_bcp_exec_imp
            IF @xp_cmdshell_result_imp = 0
            BEGIN
                SET @end_time_exec = GETDATE()
                UPDATE tdmc SET tdmc.execStartDate = @start_time_exec_imp, tdmc.execEndDate =@end_time_exec, tdmc.statusLastExecIsError = @xp_cmdshell_result_imp
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
