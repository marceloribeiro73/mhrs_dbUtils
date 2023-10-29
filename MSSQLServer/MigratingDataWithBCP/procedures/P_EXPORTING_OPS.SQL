CREATE OR ALTER PROCEDURE p_exporting_operation
@p_user_name_source NVARCHAR(128)
,@p_passwd_source NVARCHAR(128)
,@p_db_schema_table_souce NVARCHAR(128)
,@p_db_schema_table_destination NVARCHAR(128)
,@p_qtd_operations_per_exec INT
,@p_delay_between_operations CHAR(8)

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
    IF @p_user_name_source IS NULL
    BEGIN
        SET @cod_error =1
        PRINT @msg_error + 'Parameter @p_user_name_source is null'
    END
    ELSE IF (SELECT principal_id FROM sys.server_principals WHERE [name] = @p_user_name_source) IS NULL
    BEGIN
        SET @cod_error = 1
        PRINT @msg_error + 'Login not exists'
    END
    ELSE IF (SELECT is_disabled FROM sys.server_principals WHERE [name] = @p_user_name_source) = 1
    BEGIN
        SET @cod_error = 1
        PRINT @msg_error + 'Login is disabled'
    END
    IF @p_passwd_source IS NULL
    BEGIN
        SET @cod_error = 1
        PRINT @mss_error + 'Parameter @p_passwd_source is null'
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
            DECLARE @validate_cmd NVARCHAR(512) = 'SELECT TOP 1 * FROM ' + @p_db_schema_table_destination
            EXEC (@validate_cmd)
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
        DECLARE @cmd_bpc_exec NVARCHAR(MAX), @id_bpc_cmd INT, @start_time_exec DATETIME, @end_time_exec DATETIME, @xp_cmdShell_result INT
        DECLARE cur_export_ops CURSOR FAST_FORWARD FOR
            SELECT TOP (@p_qtd_operations_per_exec)
                tdmc.command
                ,tdmc.idCommand
            FROM dbo.tmp_data_migration_control tdmc
            WHERE tdmc.exporting = 1
                AND tdmc.dbSchemaTableSource = @p_db_schema_table_souce
                AND tdmc.dbSchemaTableDestination = @p_db_schema_table_destination
            ORDER BY tdmc.lowerBoundary ASC
        OPEN cur_export_ops
        FETCH NEXT FROM cur_export_ops
            INTO @cmd_bpc_exec, @id_bpc_cmd
        WHILE @@FETCH_STATUS = 0
        BEGIN
            PRINT 'Executing the command: ' + @cmd_bpc_exec
            SET @cmd_bpc_exec = CONCAT(@cmd_bpc_exec, ' -U ', @p_user_name_source, ' -P ', @p_passwd_source)
            SET @start_time_exec = GETDATE()
            EXEC @xp_cmdShell_result = xp_cmdShell @cmd_bpc_exec;
            IF @xp_cmdShell_result = 0
            BEGIN
                SET @end_time_exec = GETDATE()
                UPDATE tdmc SET tdmc.execStartDate = @start_time_exec, tdmc.execEndDate = @end_time_exec, a.statusLastExecIsError= @xp_cmdShell_result
                FROM dbo.tmp_data_migration_control tdmc
                WHERE tdmc.idCommand = @id_bpc_cmd
                PRINT 'Execution succeed'
            END
            ELSE 
            BEGIN
                UPDATE tdmc SET tdmc.execStartDate = @start_time_exec, tdmc.statusLastExecIsError = @xp_cmdShell_result
                FROM dbo.tmp_data_migration_control tdmc
                WHERE tdmc.idCommand = @id_bpc_cmd
                PRINT 'Execution failed'
            END
            WAITFOR DELAY @p_delay_between_operations
            FETCH NEXT FROM cur_export_ops
                INTO @cmd_bpc_exec, @id_bpc_cmd
        END
        CLOSE cur_export_ops
        DEALLOCATE cur_export_ops
    END
END
