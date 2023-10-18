/*
** Procedure para alterar os modos de sinconismo e de failover de grupos de disponibilidade
** Autor: Marcelo H R Silva - https://github.com/marceloribeiro73
**
*/


CREATE PROCEDURE pr_change_syncMode_failoverMode
@syncModeDesired CHAR(1) -- 'A' for Async | 'S' for Sync
,@failoverModeDesired CHAR(1) -- 'A' for Auto | 'M' for Manual
,@availabilityGroups VARCHAR(MAX) -- 'ALL' for all AGs | 'AG_ONE, AG_TWO' for specific groups
AS
BEGIN
    -- Check is HA is enabled on this instance
    DECLARE @isHaEnabled INT =(SELECT SERVERPROPERTY ('IsHadrEnabled'))

    --Check Availability Groups filter
    CREATE TABLE @AVGs (
        groupId UNIQUEIDENTIFIER,
        groupName NVARCHAR(128),
        replicaNameServer NVARCHAR(128),
        availabilityModeDesc NVARCHAR(128),
        failoverModeDesc NVARCHAR(128)
    )
    IF @availabilityGroups IS NULL or @availabilityGroups ='ALL'
    BEGIN
        INSERT INTO @AVGs
        SELECT 
            a.group_id AS 'groupId'
	        ,b.name AS 'groupName'
	        ,a.replica_server_name AS 'replicaNameServer'
	        ,a.availability_mode_desc AS 'availabilityModeDesc'
	        ,a.failover_mode_desc AS 'failoverModeDesc'
        FROM sys.availability_replicas a
        JOIN sys.availability_groups b
	        ON a.group_id = b.group_id
    END
    ELSE
    BEGIN
        CREATE TABLE @avgFilters(
            groupNames NVARCHAR(128)
        )

        INSERT INTO @avgFilters
        SELECT 
            LTRIM(RTRIM(VALUE)) as 'groupNames'
        FROM STRING_SPLIT(@availabilityGroups, ',')

        INSERT INTO @AVGs
        SELECT 
            a.group_id AS 'groupId'
	        ,b.name AS 'groupName'
	        ,a.replica_server_name AS 'replicaNameServer'
	        ,a.availability_mode_desc AS 'availabilityModeDesc'
	        ,a.failover_mode_desc AS 'failoverModeDesc'
        FROM sys.availability_replicas a
        JOIN sys.availability_groups b
	        ON a.group_id = b.group_id
        JOIN @avgFilters c
            ON b.name = c.groupNames
    END

END
