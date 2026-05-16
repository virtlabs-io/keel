-- =============================================================================
-- MySQL Group Replication — Bootstrap (Node 1 only)
-- Runs AFTER init-node1.sql
-- =============================================================================

SET GLOBAL group_replication_bootstrap_group = ON;
START GROUP_REPLICATION;
SET GLOBAL group_replication_bootstrap_group = OFF;
