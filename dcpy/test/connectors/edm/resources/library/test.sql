SET standard_conforming_strings = ON;
DROP TABLE IF EXISTS "public"."test" CASCADE;
BEGIN;
CREATE TABLE "public"."test"();
ALTER TABLE "public"."test" ADD COLUMN "ogc_fid" SERIAL CONSTRAINT "test_pk" PRIMARY KEY;
ALTER TABLE "public"."test" ADD COLUMN "a" VARCHAR;
ALTER TABLE "public"."test" ADD COLUMN "b" VARCHAR;
COPY "public"."test" ("a", "b") FROM STDIN;
1	2
\.
COMMIT;
