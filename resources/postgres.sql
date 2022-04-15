-- Converted by db_converter
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
