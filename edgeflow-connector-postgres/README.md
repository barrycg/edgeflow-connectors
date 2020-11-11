postgreSql的offset相关信息的建表语句。
CREATE TABLE "public"."edgeflow_offset_store" (
  "id" int4 NOT NULL DEFAULT nextval('edgeflow_offset_store_id_seq'::regclass),
  "namespace" varchar(128) COLLATE "pg_catalog"."default" NOT NULL DEFAULT NULL,
  "topic" varchar(128) COLLATE "pg_catalog"."default" NOT NULL DEFAULT NULL,
  "offset_timestamp" timestamp(6) DEFAULT NULL::timestamp without time zone,
  "offset_incrementing" varchar(128) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  "created_at" timestamp(6) DEFAULT NULL::timestamp without time zone,
  "created_by" varchar(128) COLLATE "pg_catalog"."default" DEFAULT NULL::character varying,
  CONSTRAINT "edgeflow_offset_store_pkey" PRIMARY KEY ("id")
)
;