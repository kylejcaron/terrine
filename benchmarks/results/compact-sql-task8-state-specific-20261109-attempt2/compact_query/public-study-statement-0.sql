SELECT COUNT(*) AS "invalid_count" FROM (SELECT * FROM "users" AS "t0" WHERE "t0"."unit_id" IS NULL) AS "t1"
