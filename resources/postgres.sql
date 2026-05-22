-- ckman PostgreSQL initial schema
-- 注意：本脚本仅用于「首次部署」，会 DROP 同名表。升级场景请勿直接执行。
START TRANSACTION;

DROP TABLE IF EXISTS "tbl_cluster";
CREATE TABLE "tbl_cluster" (
    "id" bigint ,
    "created_at" timestamp with time zone ,
    "updated_at" timestamp with time zone ,
    "deleted_at" timestamp with time zone ,
    "cluster_name" varchar(382) ,
    "config" text ,
    PRIMARY KEY ("id"),
    UNIQUE ("cluster_name")
);

DROP TABLE IF EXISTS "tbl_logic";
CREATE TABLE "tbl_logic" (
    "id" bigint ,
    "created_at" timestamp with time zone ,
    "updated_at" timestamp with time zone ,
    "deleted_at" timestamp with time zone ,
    "logic_name" varchar(382) ,
    "physic_clusters" text ,
    PRIMARY KEY ("id"),
    UNIQUE ("logic_name")
);

DROP TABLE IF EXISTS "tbl_query_history";
CREATE TABLE "tbl_query_history" (
    "cluster" varchar(382) ,
    "checksum" varchar(382) ,
    "query" text ,
    "create_time" timestamp with time zone ,
    PRIMARY KEY ("checksum")
);

DROP TABLE IF EXISTS "tbl_task";
CREATE TABLE "tbl_task" (
    "task_id" varchar(382) ,
    "status" bigint ,
    "config" text ,
    PRIMARY KEY ("task_id")
);

-- 3.x 老备份表，4.0.0 仍保留只读用于 `ckmanctl upgrade backup` 数据迁移
DROP TABLE IF EXISTS "tbl_backup";
CREATE TABLE "tbl_backup" (
    "backup_id" varchar(382) ,
    "cluster_name" varchar(382) ,
    "update_time" varchar(32) ,
    "backup" text ,
    PRIMARY KEY ("backup_id")
);

-- 4.0.0 新增：备份策略 / 运行记录
-- 字段定义须与 repository/postgres/postgres.go 中 CREATE TABLE IF NOT EXISTS 保持一致
DROP TABLE IF EXISTS "tbl_backup_policy";
CREATE TABLE "tbl_backup_policy" (
    "policy_id"     VARCHAR(64) PRIMARY KEY,
    "cluster_name"  VARCHAR(128) NOT NULL,
    "database_name" VARCHAR(128) NOT NULL,
    "table_name"    VARCHAR(128) NOT NULL,
    "instance"      VARCHAR(64),
    "schedule_type" VARCHAR(16),
    "enabled"       BOOLEAN,
    "deleted"       BOOLEAN,
    "policy"        JSONB,
    "update_time"   VARCHAR(32)
);
CREATE INDEX IF NOT EXISTS idx_bp_cluster_db_table
    ON tbl_backup_policy(cluster_name, database_name, table_name);
CREATE INDEX IF NOT EXISTS idx_bp_instance
    ON tbl_backup_policy(instance);

DROP TABLE IF EXISTS "tbl_backup_run";
CREATE TABLE "tbl_backup_run" (
    "run_id"        VARCHAR(64) PRIMARY KEY,
    "policy_id"     VARCHAR(64) NOT NULL,
    "cluster_name"  VARCHAR(128) NOT NULL,
    "database_name" VARCHAR(128) NOT NULL,
    "table_name"    VARCHAR(128) NOT NULL,
    "status"        VARCHAR(16),
    "instance"      VARCHAR(64),
    "started_at"    TIMESTAMP,
    "run"           JSONB,
    "create_time"   TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_br_policy_started
    ON tbl_backup_run(policy_id, started_at);
CREATE INDEX IF NOT EXISTS idx_br_table_started
    ON tbl_backup_run(cluster_name, database_name, table_name, started_at);
CREATE INDEX IF NOT EXISTS idx_br_status_instance
    ON tbl_backup_run(status, instance);

-- 4.0.0 新增：用户管理（Phase 1）
DROP TABLE IF EXISTS "tbl_user";
CREATE TABLE "tbl_user" (
    "id"            SERIAL PRIMARY KEY,
    "created_at"    TIMESTAMP,
    "updated_at"    TIMESTAMP,
    "username"      VARCHAR(32) NOT NULL,
    "password_hash" VARCHAR(64) NOT NULL,
    "policy"        VARCHAR(16) NOT NULL,
    "enabled"       BOOLEAN NOT NULL DEFAULT true
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_user_name ON tbl_user(username);

-- Post-data save --
COMMIT;
START TRANSACTION;

-- Foreign keys --

-- Sequences --
CREATE SEQUENCE tbl_cluster_id_seq;
SELECT setval('tbl_cluster_id_seq', max(id)) FROM tbl_cluster;
ALTER TABLE "tbl_cluster" ALTER COLUMN "id" SET DEFAULT nextval('tbl_cluster_id_seq');
CREATE SEQUENCE tbl_logic_id_seq;
SELECT setval('tbl_logic_id_seq', max(id)) FROM tbl_logic;
ALTER TABLE "tbl_logic" ALTER COLUMN "id" SET DEFAULT nextval('tbl_logic_id_seq');

-- Full Text keys --

COMMIT;
