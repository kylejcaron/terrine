SELECT COUNT(*) AS "invalid_count" FROM (SELECT * FROM "customers" AS "t0" WHERE "t0"."customer_id" IS NULL) AS "t1"
