WITH "t12" AS (
  SELECT
    "t10"."unit",
    "t10"."anchor_id",
    "t10"."t0",
    GREATEST(
      MAKE_TIMESTAMPTZ(2024, 12, 22, 0, 0, 0.0, 'UTC'),
      COALESCE("t10"."_observed_from", MAKE_TIMESTAMPTZ(2024, 12, 22, 0, 0, 0.0, 'UTC'))
    ) AS "__terrine_w3_of",
    LEAST(
      MAKE_TIMESTAMPTZ(2025, 2, 3, 0, 0, 0.0, 'UTC'),
      COALESCE("t10"."_observed_until", MAKE_TIMESTAMPTZ(2025, 2, 3, 0, 0, 0.0, 'UTC'))
    ) AS "__terrine_w4_ou",
    (
      (
        "t10"."t0" + INTERVAL '-604800' SECOND
      ) >= MAKE_TIMESTAMPTZ(2024, 12, 22, 0, 0, 0.0, 'UTC')
    )
    AND (
      "t10"."t0" <= MAKE_TIMESTAMPTZ(2025, 2, 3, 0, 0, 0.0, 'UTC')
    ) AS "__terrine_w2_sc",
    (
      (
        "t10"."_observed_from" IS NULL
      )
      OR (
        (
          "t10"."t0" + INTERVAL '-604800' SECOND
        ) >= "t10"."_observed_from"
      )
    )
    AND (
      (
        "t10"."_observed_until" IS NULL
      )
      OR (
        "t10"."t0" <= "t10"."_observed_until"
      )
    ) AS "__terrine_w5_o"
  FROM (
    SELECT
      "t8"."unit",
      "t6"."anchor_id",
      "t6"."t0",
      "t8"."observed_from_utc" AS "_observed_from",
      "t8"."observed_until_utc" AS "_observed_until"
    FROM (
      SELECT
        "t0"."unit_id" AS "unit",
        CAST("t0"."observed_from" AS TIMESTAMPTZ) AS "observed_from_utc",
        CAST("t0"."observed_until" AS TIMESTAMPTZ) AS "observed_until_utc"
      FROM "users" AS "t0"
    ) AS "t8"
    CROSS JOIN (
      SELECT
        'fixed' AS "anchor_id",
        MAKE_TIMESTAMPTZ(2025, 1, 22, 0, 0, 0.0, 'UTC') AS "t0"
      FROM (
        SELECT
          1 AS "1"
      ) AS "t2"
    ) AS "t6"
  ) AS "t10"
), "t16" AS (
  SELECT
    "t8"."unit",
    "t15"."anchor_id",
    "t15"."t0",
    "t8"."observed_from_utc" AS "_observed_from",
    "t8"."observed_until_utc" AS "_observed_until"
  FROM (
    SELECT
      "t0"."unit_id" AS "unit",
      CAST("t0"."observed_from" AS TIMESTAMPTZ) AS "observed_from_utc",
      CAST("t0"."observed_until" AS TIMESTAMPTZ) AS "observed_until_utc"
    FROM "users" AS "t0"
  ) AS "t8"
  CROSS JOIN (
    SELECT
      *
    FROM (
      SELECT
        'schedule:' || CAST("t9"."ordinal" AS TEXT) AS "anchor_id",
        MAKE_TIMESTAMPTZ(2025, 1, 8, 0, 0, 0.0, 'UTC') + TO_SECONDS("t9"."ordinal" * 604800) AS "t0"
      FROM (
        SELECT
          CAST(UNNEST(RANGE(0, 2, 1)) AS BIGINT) AS "ordinal"
      ) AS "t9"
    ) AS "t11"
    WHERE
      "t11"."t0" < MAKE_TIMESTAMPTZ(2025, 1, 22, 0, 0, 0.0, 'UTC')
  ) AS "t15"
), "t26" AS (
  SELECT
    "t24"."unit",
    "t24"."anchor_id",
    "t24"."__terrine_w0_s",
    "t24"."__terrine_w1_e",
    "t24"."__terrine_w5_of",
    "t24"."__terrine_w6_ou",
    (
      "t24"."t0" IS NOT NULL
    )
    AND NOT (
      (
        "t24"."__terrine_w0_s" = TIMEZONE('UTC', CAST(CAST(TIMEZONE('UTC', "t24"."__terrine_w0_s") AS DATE) AS TIMESTAMP))
      )
      AND (
        "t24"."__terrine_w1_e" = TIMEZONE('UTC', CAST(CAST(TIMEZONE('UTC', "t24"."__terrine_w1_e") AS DATE) AS TIMESTAMP))
      )
    ) AS "__terrine_w2_u",
    (
      "t24"."__terrine_w0_s" >= MAKE_TIMESTAMPTZ(2024, 12, 22, 0, 0, 0.0, 'UTC')
    )
    AND (
      "t24"."__terrine_w1_e" <= MAKE_TIMESTAMPTZ(2025, 2, 3, 0, 0, 0.0, 'UTC')
    ) AS "__terrine_w4_sc",
    (
      (
        "t24"."_observed_from" IS NULL
      )
      OR (
        "t24"."__terrine_w0_s" >= "t24"."_observed_from"
      )
    )
    AND (
      (
        "t24"."_observed_until" IS NULL
      )
      OR (
        "t24"."__terrine_w1_e" <= "t24"."_observed_until"
      )
    ) AS "__terrine_w7_o"
  FROM (
    SELECT
      "t18"."unit",
      "t18"."anchor_id",
      "t18"."t0",
      "t18"."_observed_from",
      "t18"."_observed_until",
      "t18"."t0" + INTERVAL '-604800' SECOND AS "__terrine_w0_s",
      "t18"."t0" AS "__terrine_w1_e",
      GREATEST(
        MAKE_TIMESTAMPTZ(2024, 12, 22, 0, 0, 0.0, 'UTC'),
        COALESCE("t18"."_observed_from", MAKE_TIMESTAMPTZ(2024, 12, 22, 0, 0, 0.0, 'UTC'))
      ) AS "__terrine_w5_of",
      LEAST(
        MAKE_TIMESTAMPTZ(2025, 2, 3, 0, 0, 0.0, 'UTC'),
        COALESCE("t18"."_observed_until", MAKE_TIMESTAMPTZ(2025, 2, 3, 0, 0, 0.0, 'UTC'))
      ) AS "__terrine_w6_ou"
    FROM "t16" AS "t18"
  ) AS "t24"
), "t46" AS (
  SELECT
    "t43"."_unit",
    "t43"."value",
    TIMEZONE(
      'UTC',
      CAST(CAST(TIMEZONE('UTC', "t43"."__terrine_w18_dt") AS DATE) AS TIMESTAMP) - INTERVAL '0' MINUTE
    ) AS "__terrine_w10_b"
  FROM (
    SELECT
      "t41"."_unit",
      "t41"."value",
      CASE
        WHEN COALESCE(
          (
            "t41"."_timestamp" >= CAST("t41"."__terrine_w16_ss" AS TIMESTAMPTZ)
          )
          AND (
            "t41"."_timestamp" < CAST("t41"."__terrine_w17_se" AS TIMESTAMPTZ)
          ),
          FALSE
        )
        THEN "t41"."_timestamp"
        ELSE CAST(NULL AS TIMESTAMPTZ)
      END AS "__terrine_w18_dt"
    FROM (
      SELECT
        "t7"."_timestamp",
        "t7"."_unit",
        "t7"."value",
        "t38"."__terrine_w16_ss",
        "t38"."__terrine_w17_se"
      FROM (
        SELECT
          "t1"."event_time" AS "_timestamp",
          "t1"."unit_id" AS "_unit",
          "t1"."value"
        FROM "events" AS "t1"
      ) AS "t7"
      LEFT OUTER JOIN (
        SELECT
          "t34"."__terrine_w15_su",
          MIN("t34"."__terrine_w16_ss") AS "__terrine_w16_ss",
          MAX("t34"."__terrine_w17_se") AS "__terrine_w17_se"
        FROM (
          SELECT
            "t27"."unit" AS "__terrine_w15_su",
            LEAST(
              CASE
                WHEN COALESCE("t27"."__terrine_w4_sc" AND "t27"."__terrine_w7_o", FALSE)
                AND NOT (
                  COALESCE(COALESCE("t27"."__terrine_w2_u", TRUE), TRUE)
                )
                THEN "t27"."__terrine_w0_s"
                ELSE CAST(NULL AS TIMESTAMPTZ)
              END
            ) AS "__terrine_w16_ss",
            GREATEST(
              CASE
                WHEN COALESCE("t27"."__terrine_w4_sc" AND "t27"."__terrine_w7_o", FALSE)
                AND NOT (
                  COALESCE(COALESCE("t27"."__terrine_w2_u", TRUE), TRUE)
                )
                THEN "t27"."__terrine_w1_e"
                ELSE CAST(NULL AS TIMESTAMPTZ)
              END
            ) AS "__terrine_w17_se"
          FROM "t26" AS "t27"
        ) AS "t34"
        GROUP BY
          1
      ) AS "t38"
        ON "t7"."_unit" = "t38"."__terrine_w15_su"
    ) AS "t41"
  ) AS "t43"
), "t72" AS (
  SELECT
    "t71"."unit",
    "t71"."t0",
    "t71"."_state_period",
    "t71"."__terrine_value_0_observed_from",
    "t71"."__terrine_value_0_observed_until",
    COALESCE("t71"."_state_period__complete", FALSE) AS "__terrine_state_complete",
    "t71"."_state_period__reason" AS "__terrine_state_reason"
  FROM (
    SELECT
      *
    FROM (
      SELECT
        "t8"."unit",
        "t15"."t0",
        "t69"."__terrine_value_0" AS "_state_period",
        "t69"."__terrine_value_0_complete" AS "_state_period__complete",
        "t69"."__terrine_value_0_reason" AS "_state_period__reason",
        "t69"."__terrine_value_0_observed_from",
        "t69"."__terrine_value_0_observed_until"
      FROM (
        SELECT
          "t0"."unit_id" AS "unit",
          CAST("t0"."observed_from" AS TIMESTAMPTZ) AS "observed_from_utc",
          CAST("t0"."observed_until" AS TIMESTAMPTZ) AS "observed_until_utc"
        FROM "users" AS "t0"
      ) AS "t8"
      CROSS JOIN (
        SELECT
          *
        FROM (
          SELECT
            'schedule:' || CAST("t9"."ordinal" AS TEXT) AS "anchor_id",
            MAKE_TIMESTAMPTZ(2025, 1, 8, 0, 0, 0.0, 'UTC') + TO_SECONDS("t9"."ordinal" * 604800) AS "t0"
          FROM (
            SELECT
              CAST(UNNEST(RANGE(0, 2, 1)) AS BIGINT) AS "ordinal"
          ) AS "t9"
        ) AS "t11"
        WHERE
          "t11"."t0" < MAKE_TIMESTAMPTZ(2025, 1, 22, 0, 0, 0.0, 'UTC')
      ) AS "t15"
      LEFT OUTER JOIN (
        SELECT
          "t67"."unit" AS "__terrine_value_0_key_unit",
          "t67"."anchor_id" AS "__terrine_value_0_key_anchor_id",
          CASE
            WHEN "t67"."__terrine_w33_r" IS NULL
            THEN "t67"."__terrine_w35_eo"
            ELSE CAST(NULL AS DECIMAL(38, 0))
          END AS "__terrine_value_0",
          "t67"."__terrine_w5_of" AS "__terrine_value_0_observed_from",
          "t67"."__terrine_w6_ou" AS "__terrine_value_0_observed_until",
          "t67"."__terrine_w8_c" AS "__terrine_value_0_complete",
          "t67"."__terrine_w33_r" AS "__terrine_value_0_reason"
        FROM (
          SELECT
            "t66"."unit",
            "t66"."anchor_id",
            "t66"."__terrine_w5_of",
            "t66"."__terrine_w6_ou",
            "t66"."__terrine_w8_c",
            TRY_CAST(CAST(COALESCE("t66"."__terrine_w29_sv", CAST('0' AS BIGNUM)) AS TEXT) AS DECIMAL(38, 0)) AS "__terrine_w35_eo",
            CASE
              WHEN "t66"."__terrine_w8_c"
              THEN CASE
                WHEN (
                  COALESCE("t66"."__terrine_w27_ec", CAST('0' AS INT128)) > CAST('0' AS INT128)
                )
                AND (
                  COALESCE("t66"."__terrine_w28_nc", CAST('0' AS INT128)) = CAST('0' AS INT128)
                )
                THEN 'all_values_null'
                ELSE CAST(NULL AS TEXT)
              END
              ELSE "t66"."__terrine_w9_ur"
            END AS "__terrine_w33_r"
          FROM (
            SELECT
              "t65".*
            FROM (
              SELECT
                "t32"."unit",
                "t32"."anchor_id",
                "t32"."__terrine_w5_of",
                "t32"."__terrine_w6_ou",
                "t32"."__terrine_w8_c",
                "t32"."__terrine_w9_ur",
                "t64"."__terrine_w27_ec",
                "t64"."__terrine_w28_nc",
                "t64"."__terrine_w29_sv"
              FROM (
                SELECT
                  "t27"."unit",
                  "t27"."anchor_id",
                  "t27"."__terrine_w0_s",
                  "t27"."__terrine_w1_e",
                  "t27"."__terrine_w5_of",
                  "t27"."__terrine_w6_ou",
                  COALESCE("t27"."__terrine_w2_u", TRUE) AS "__terrine_w3_c",
                  "t27"."__terrine_w4_sc" AND "t27"."__terrine_w7_o" AS "__terrine_w8_c",
                  CASE
                    WHEN NOT (
                      "t27"."__terrine_w4_sc"
                    )
                    THEN 'incomplete_coverage'
                    ELSE CASE
                      WHEN NOT (
                        "t27"."__terrine_w7_o"
                      )
                      THEN 'incomplete_observation'
                      ELSE CAST(NULL AS TEXT)
                    END
                  END AS "__terrine_w9_ur"
                FROM "t26" AS "t27"
              ) AS "t32"
              LEFT OUTER JOIN (
                SELECT
                  "t62"."__terrine_w13_u",
                  "t62"."__terrine_w14_ai",
                  SUM("t62"."__terrine_w27_ec") AS "__terrine_w27_ec",
                  SUM("t62"."__terrine_w28_nc") AS "__terrine_w28_nc",
                  SUM("t62"."__terrine_w29_sv") AS "__terrine_w29_sv"
                FROM (
                  SELECT
                    "t61"."__terrine_w13_u",
                    "t61"."__terrine_w14_ai",
                    CASE
                      WHEN "t61"."__terrine_w26_c"
                      THEN "t61"."__terrine_w23_ec"
                      ELSE CAST(NULL AS INT128)
                    END AS "__terrine_w27_ec",
                    CASE
                      WHEN "t61"."__terrine_w26_c"
                      THEN "t61"."__terrine_w24_nc"
                      ELSE CAST(NULL AS INT128)
                    END AS "__terrine_w28_nc",
                    CASE
                      WHEN "t61"."__terrine_w26_c"
                      THEN "t61"."__terrine_w25_sv"
                      ELSE CAST(NULL AS BIGNUM)
                    END AS "__terrine_w29_sv"
                  FROM (
                    SELECT
                      "t60"."__terrine_w23_ec",
                      "t60"."__terrine_w24_nc",
                      "t60"."__terrine_w25_sv",
                      "t60"."__terrine_w13_u",
                      "t60"."__terrine_w14_ai",
                      (
                        FALSE OR "t60"."__terrine_w8_c"
                      )
                      AND (
                        "t60"."__terrine_w12_sb" >= "t60"."__terrine_w0_s"
                      )
                      AND (
                        "t60"."__terrine_w12_sb" < "t60"."__terrine_w1_e"
                      ) AS "__terrine_w26_c"
                    FROM (
                      SELECT
                        "t59"."__terrine_w12_sb",
                        "t59"."__terrine_w23_ec",
                        "t59"."__terrine_w24_nc",
                        "t59"."__terrine_w25_sv",
                        "t30"."__terrine_w13_u",
                        "t30"."__terrine_w14_ai",
                        "t30"."__terrine_w8_c",
                        "t30"."__terrine_w0_s",
                        "t30"."__terrine_w1_e"
                      FROM (
                        SELECT
                          "t57"."__terrine_w11_su",
                          "t57"."__terrine_w12_sb",
                          SUM("t57"."__terrine_w23_ec") AS "__terrine_w23_ec",
                          SUM("t57"."__terrine_w24_nc") AS "__terrine_w24_nc",
                          SUM("t57"."__terrine_w25_sv") AS "__terrine_w25_sv"
                        FROM (
                          SELECT
                            "t56"."__terrine_w11_su",
                            "t56"."__terrine_w12_sb",
                            CASE WHEN "t56"."__terrine_w22_s" THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END AS "__terrine_w23_ec",
                            CASE
                              WHEN "t56"."__terrine_w22_s" AND (
                                "t56"."value" IS NOT NULL
                              )
                              THEN CAST(1 AS BIGINT)
                              ELSE CAST(0 AS BIGINT)
                            END AS "__terrine_w24_nc",
                            CASE
                              WHEN "t56"."__terrine_w22_s"
                              THEN CAST(CAST("t56"."value" AS TEXT) AS BIGNUM)
                              ELSE CAST(NULL AS BIGNUM)
                            END AS "__terrine_w25_sv"
                          FROM (
                            SELECT
                              "t55"."value",
                              "t55"."__terrine_w11_su",
                              "t55"."__terrine_w12_sb",
                              COALESCE("t55"."__terrine_w19_c", FALSE) AND COALESCE(TRUE, FALSE) AS "__terrine_w22_s"
                            FROM (
                              SELECT
                                "t49"."value",
                                "t54"."__terrine_w11_su",
                                "t54"."__terrine_w12_sb",
                                "t54"."__terrine_w19_c"
                              FROM "t46" AS "t49"
                              INNER JOIN (
                                SELECT
                                  "t52"."__terrine_w11_su",
                                  "t52"."__terrine_w12_sb",
                                  BOOL_OR(CAST("t52"."__terrine_w19_c" AS BOOLEAN)) AS "__terrine_w19_c"
                                FROM (
                                  SELECT
                                    "t51"."__terrine_w11_su",
                                    "t51"."__terrine_w12_sb",
                                    FALSE
                                    OR (
                                      (
                                        FALSE OR "t30"."__terrine_w8_c"
                                      )
                                      AND (
                                        "t51"."__terrine_w12_sb" >= "t30"."__terrine_w0_s"
                                      )
                                      AND (
                                        "t51"."__terrine_w12_sb" < "t30"."__terrine_w1_e"
                                      )
                                    ) AS "__terrine_w19_c"
                                  FROM (
                                    SELECT DISTINCT
                                      "t47"."_unit" AS "__terrine_w11_su",
                                      "t47"."__terrine_w10_b" AS "__terrine_w12_sb"
                                    FROM "t46" AS "t47"
                                  ) AS "t51"
                                  INNER JOIN (
                                    SELECT
                                      "t27"."unit" AS "__terrine_w13_u",
                                      "t27"."anchor_id" AS "__terrine_w14_ai",
                                      "t27"."__terrine_w4_sc" AND "t27"."__terrine_w7_o" AS "__terrine_w8_c",
                                      "t27"."__terrine_w0_s",
                                      "t27"."__terrine_w1_e"
                                    FROM "t26" AS "t27"
                                  ) AS "t30"
                                    ON "t51"."__terrine_w11_su" = "t30"."__terrine_w13_u"
                                    AND (
                                      FALSE
                                      OR (
                                        (
                                          FALSE OR "t30"."__terrine_w8_c"
                                        )
                                        AND (
                                          "t51"."__terrine_w12_sb" >= "t30"."__terrine_w0_s"
                                        )
                                        AND (
                                          "t51"."__terrine_w12_sb" < "t30"."__terrine_w1_e"
                                        )
                                      )
                                    )
                                ) AS "t52"
                                GROUP BY
                                  1,
                                  2
                              ) AS "t54"
                                ON "t49"."_unit" = "t54"."__terrine_w11_su"
                                AND "t49"."__terrine_w10_b" = "t54"."__terrine_w12_sb"
                            ) AS "t55"
                          ) AS "t56"
                        ) AS "t57"
                        GROUP BY
                          1,
                          2
                      ) AS "t59"
                      INNER JOIN (
                        SELECT
                          "t27"."unit" AS "__terrine_w13_u",
                          "t27"."anchor_id" AS "__terrine_w14_ai",
                          "t27"."__terrine_w4_sc" AND "t27"."__terrine_w7_o" AS "__terrine_w8_c",
                          "t27"."__terrine_w0_s",
                          "t27"."__terrine_w1_e"
                        FROM "t26" AS "t27"
                      ) AS "t30"
                        ON "t59"."__terrine_w11_su" = "t30"."__terrine_w13_u"
                        AND (
                          FALSE
                          OR (
                            (
                              FALSE OR "t30"."__terrine_w8_c"
                            )
                            AND (
                              "t59"."__terrine_w12_sb" >= "t30"."__terrine_w0_s"
                            )
                            AND (
                              "t59"."__terrine_w12_sb" < "t30"."__terrine_w1_e"
                            )
                          )
                        )
                    ) AS "t60"
                  ) AS "t61"
                ) AS "t62"
                GROUP BY
                  1,
                  2
              ) AS "t64"
                ON "t32"."unit" = "t64"."__terrine_w13_u"
                AND "t32"."anchor_id" = "t64"."__terrine_w14_ai"
            ) AS "t65"
          ) AS "t66"
        ) AS "t67"
      ) AS "t69"
        ON "t8"."unit" IS NOT DISTINCT FROM "t69"."__terrine_value_0_key_unit"
        AND "t15"."anchor_id" IS NOT DISTINCT FROM "t69"."__terrine_value_0_key_anchor_id"
    ) AS "t70"
    UNION ALL
    SELECT
      *
    FROM (
      SELECT
        "t8"."unit",
        "t6"."t0",
        "t45"."__terrine_value_0" AS "_state_period",
        "t45"."__terrine_value_0_complete" AS "_state_period__complete",
        "t45"."__terrine_value_0_reason" AS "_state_period__reason",
        "t45"."__terrine_value_0_observed_from",
        "t45"."__terrine_value_0_observed_until"
      FROM (
        SELECT
          "t0"."unit_id" AS "unit",
          CAST("t0"."observed_from" AS TIMESTAMPTZ) AS "observed_from_utc",
          CAST("t0"."observed_until" AS TIMESTAMPTZ) AS "observed_until_utc"
        FROM "users" AS "t0"
      ) AS "t8"
      CROSS JOIN (
        SELECT
          'fixed' AS "anchor_id",
          MAKE_TIMESTAMPTZ(2025, 1, 22, 0, 0, 0.0, 'UTC') AS "t0"
        FROM (
          SELECT
            1 AS "1"
        ) AS "t2"
      ) AS "t6"
      LEFT OUTER JOIN (
        SELECT
          "t42"."unit" AS "__terrine_value_0_key_unit",
          "t42"."anchor_id" AS "__terrine_value_0_key_anchor_id",
          CASE
            WHEN "t42"."__terrine_w22_r" IS NULL
            THEN "t42"."__terrine_w24_eo"
            ELSE CAST(NULL AS DECIMAL(38, 0))
          END AS "__terrine_value_0",
          "t42"."__terrine_w3_of" AS "__terrine_value_0_observed_from",
          "t42"."__terrine_w4_ou" AS "__terrine_value_0_observed_until",
          "t42"."__terrine_w6_c" AS "__terrine_value_0_complete",
          "t42"."__terrine_w22_r" AS "__terrine_value_0_reason"
        FROM (
          SELECT
            "t40"."unit",
            "t40"."anchor_id",
            "t40"."__terrine_w3_of",
            "t40"."__terrine_w4_ou",
            "t40"."__terrine_w6_c",
            TRY_CAST(CAST(COALESCE("t40"."__terrine_w18_sv", CAST('0' AS BIGNUM)) AS TEXT) AS DECIMAL(38, 0)) AS "__terrine_w24_eo",
            CASE
              WHEN "t40"."__terrine_w6_c"
              THEN CASE
                WHEN (
                  COALESCE("t40"."__terrine_w16_ec", CAST('0' AS INT128)) > CAST('0' AS INT128)
                )
                AND (
                  COALESCE("t40"."__terrine_w17_nc", CAST('0' AS INT128)) = CAST('0' AS INT128)
                )
                THEN 'all_values_null'
                ELSE CAST(NULL AS TEXT)
              END
              ELSE "t40"."__terrine_w7_ur"
            END AS "__terrine_w22_r"
          FROM (
            SELECT
              "t39".*
            FROM (
              SELECT
                "t22"."unit",
                "t22"."anchor_id",
                "t22"."__terrine_w3_of",
                "t22"."__terrine_w4_ou",
                "t22"."__terrine_w6_c",
                "t22"."__terrine_w7_ur",
                "t36"."__terrine_w16_ec",
                "t36"."__terrine_w17_nc",
                "t36"."__terrine_w18_sv"
              FROM (
                SELECT
                  "t14"."unit",
                  "t14"."anchor_id",
                  "t14"."t0",
                  "t14"."__terrine_w3_of",
                  "t14"."__terrine_w4_ou",
                  "t14"."__terrine_w2_sc" AND "t14"."__terrine_w5_o" AS "__terrine_w6_c",
                  CASE
                    WHEN NOT (
                      "t14"."__terrine_w2_sc"
                    )
                    THEN 'incomplete_coverage'
                    ELSE CASE
                      WHEN NOT (
                        "t14"."__terrine_w5_o"
                      )
                      THEN 'incomplete_observation'
                      ELSE CAST(NULL AS TEXT)
                    END
                  END AS "__terrine_w7_ur"
                FROM "t12" AS "t14"
              ) AS "t22"
              LEFT OUTER JOIN (
                SELECT
                  "t33"."__terrine_w8_u",
                  "t33"."__terrine_w9_ai",
                  SUM("t33"."__terrine_w16_ec") AS "__terrine_w16_ec",
                  SUM("t33"."__terrine_w17_nc") AS "__terrine_w17_nc",
                  SUM("t33"."__terrine_w18_sv") AS "__terrine_w18_sv"
                FROM (
                  SELECT
                    "t28"."__terrine_w8_u",
                    "t28"."__terrine_w9_ai",
                    CASE WHEN "t28"."__terrine_w15_s" THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END AS "__terrine_w16_ec",
                    CASE
                      WHEN "t28"."__terrine_w15_s" AND (
                        "t28"."value" IS NOT NULL
                      )
                      THEN CAST(1 AS BIGINT)
                      ELSE CAST(0 AS BIGINT)
                    END AS "__terrine_w17_nc",
                    CASE
                      WHEN "t28"."__terrine_w15_s"
                      THEN CAST(CAST("t28"."value" AS TEXT) AS BIGNUM)
                      ELSE CAST(NULL AS BIGNUM)
                    END AS "__terrine_w18_sv"
                  FROM (
                    SELECT
                      "t25"."value",
                      "t25"."__terrine_w8_u",
                      "t25"."__terrine_w9_ai",
                      COALESCE(
                        (
                          FALSE OR "t25"."__terrine_w6_c"
                        )
                        AND (
                          (
                            "t25"."_timestamp" >= CAST("t25"."__terrine_w11_sp" AS TIMESTAMPTZ)
                          )
                          AND (
                            "t25"."_timestamp" < CAST("t25"."__terrine_w10_t" AS TIMESTAMPTZ)
                          )
                        ),
                        FALSE
                      )
                      AND COALESCE(TRUE, FALSE) AS "__terrine_w15_s"
                    FROM (
                      SELECT
                        "t7"."_timestamp",
                        "t7"."value",
                        "t19"."__terrine_w8_u",
                        "t19"."__terrine_w9_ai",
                        "t19"."__terrine_w10_t",
                        "t19"."__terrine_w6_c",
                        "t19"."__terrine_w11_sp"
                      FROM (
                        SELECT
                          "t1"."event_time" AS "_timestamp",
                          "t1"."unit_id" AS "_unit",
                          "t1"."value"
                        FROM "events" AS "t1"
                      ) AS "t7"
                      INNER JOIN (
                        SELECT
                          "t14"."unit" AS "__terrine_w8_u",
                          "t14"."anchor_id" AS "__terrine_w9_ai",
                          "t14"."t0" AS "__terrine_w10_t",
                          "t14"."__terrine_w2_sc" AND "t14"."__terrine_w5_o" AS "__terrine_w6_c",
                          "t14"."t0" + INTERVAL '-604800' SECOND AS "__terrine_w11_sp"
                        FROM "t12" AS "t14"
                      ) AS "t19"
                        ON "t7"."_unit" = "t19"."__terrine_w8_u"
                        AND (
                          FALSE
                          OR (
                            (
                              FALSE OR "t19"."__terrine_w6_c"
                            )
                            AND (
                              (
                                "t7"."_timestamp" >= CAST("t19"."__terrine_w11_sp" AS TIMESTAMPTZ)
                              )
                              AND (
                                "t7"."_timestamp" < CAST("t19"."__terrine_w10_t" AS TIMESTAMPTZ)
                              )
                            )
                          )
                        )
                    ) AS "t25"
                  ) AS "t28"
                ) AS "t33"
                GROUP BY
                  1,
                  2
              ) AS "t36"
                ON "t22"."unit" = "t36"."__terrine_w8_u"
                AND "t22"."anchor_id" = "t36"."__terrine_w9_ai"
            ) AS "t39"
          ) AS "t40"
        ) AS "t42"
      ) AS "t45"
        ON "t8"."unit" IS NOT DISTINCT FROM "t45"."__terrine_value_0_key_unit"
        AND "t6"."anchor_id" IS NOT DISTINCT FROM "t45"."__terrine_value_0_key_anchor_id"
    ) AS "t48"
  ) AS "t71"
)
SELECT
  "t76"."unit",
  "t76"."anchor_id",
  "t76"."t0",
  "t76"."from_time",
  "t76"."to_time",
  "t76"."scheduled_to",
  "t76"."period_index",
  "t76"."gap",
  "t76"."from_state",
  "t76"."from_state_complete" AS "from_state__complete",
  "t76"."from_state_reason" AS "from_state__reason",
  "t76"."to_state",
  "t76"."to_state_complete" AS "to_state__complete",
  "t76"."to_state_reason" AS "to_state__reason",
  "t76"."terminal",
  "t76"."from_state_observed_from",
  "t76"."from_state_observed_until",
  "t76"."to_state_observed_from",
  "t76"."to_state_observed_until",
  MAKE_TIMESTAMPTZ(2025, 1, 8, 0, 0, 0.0, 'UTC') AS "__terrine_panel_start"
FROM (
  SELECT
    "t23"."unit",
    "t23"."anchor_id",
    "t23"."t0",
    "t23"."t0" AS "from_time",
    "t23"."t0" + INTERVAL '604800' SECOND AS "to_time",
    "t23"."t0" + INTERVAL '604800' SECOND AS "scheduled_to",
    CAST(REGEXP_REPLACE("t23"."anchor_id", '^schedule:', '', 'g') AS BIGINT) AS "period_index",
    604800000000 AS "gap",
    CASE
      WHEN COALESCE("t74"."__terrine_state_complete", FALSE)
      THEN CASE
        WHEN "t74"."_state_period" <= CAST('0' AS DECIMAL(1, 0))
        THEN 'low'
        WHEN "t74"."_state_period" > CAST('0' AS DECIMAL(1, 0))
        THEN 'high'
        ELSE CAST(NULL AS TEXT)
      END
      ELSE CAST(NULL AS TEXT)
    END AS "from_state",
    CASE
      WHEN COALESCE("t75"."__terrine_state_complete", FALSE)
      THEN CASE
        WHEN "t75"."_state_period" <= CAST('0' AS DECIMAL(1, 0))
        THEN 'low'
        WHEN "t75"."_state_period" > CAST('0' AS DECIMAL(1, 0))
        THEN 'high'
        ELSE CAST(NULL AS TEXT)
      END
      ELSE CAST(NULL AS TEXT)
    END AS "to_state",
    FALSE AS "terminal",
    COALESCE("t74"."__terrine_state_complete", FALSE)
    AND (
      CASE
        WHEN "t74"."_state_period" <= CAST('0' AS DECIMAL(1, 0))
        THEN 'low'
        WHEN "t74"."_state_period" > CAST('0' AS DECIMAL(1, 0))
        THEN 'high'
        ELSE CAST(NULL AS TEXT)
      END IS NOT NULL
    ) AS "from_state_complete",
    CASE
      WHEN NOT (
        COALESCE("t74"."__terrine_state_complete", FALSE)
      )
      THEN COALESCE("t74"."__terrine_state_reason", 'incomplete_observation')
      WHEN COALESCE("t74"."__terrine_state_complete", FALSE)
      AND (
        "t74"."_state_period" IS NULL
      )
      THEN COALESCE("t74"."__terrine_state_reason", 'state_value_null')
      ELSE CAST(NULL AS TEXT)
    END AS "from_state_reason",
    COALESCE("t75"."__terrine_state_complete", FALSE)
    AND (
      CASE
        WHEN "t75"."_state_period" <= CAST('0' AS DECIMAL(1, 0))
        THEN 'low'
        WHEN "t75"."_state_period" > CAST('0' AS DECIMAL(1, 0))
        THEN 'high'
        ELSE CAST(NULL AS TEXT)
      END IS NOT NULL
    ) AS "to_state_complete",
    CASE
      WHEN NOT (
        COALESCE("t75"."__terrine_state_complete", FALSE)
      )
      THEN COALESCE("t75"."__terrine_state_reason", 'incomplete_observation')
      WHEN COALESCE("t75"."__terrine_state_complete", FALSE)
      AND (
        "t75"."_state_period" IS NULL
      )
      THEN COALESCE("t75"."__terrine_state_reason", 'state_value_null')
      ELSE CAST(NULL AS TEXT)
    END AS "to_state_reason",
    "t74"."__terrine_value_0_observed_from" AS "from_state_observed_from",
    "t74"."__terrine_value_0_observed_until" AS "from_state_observed_until",
    "t75"."__terrine_value_0_observed_from" AS "to_state_observed_from",
    "t75"."__terrine_value_0_observed_until" AS "to_state_observed_until"
  FROM (
    SELECT
      "t18"."unit",
      "t18"."anchor_id",
      "t18"."t0"
    FROM "t16" AS "t18"
  ) AS "t23"
  LEFT OUTER JOIN "t72" AS "t74"
    ON "t23"."unit" = "t74"."unit" AND "t23"."t0" = "t74"."t0"
  LEFT OUTER JOIN "t72" AS "t75"
    ON "t23"."unit" = "t75"."unit"
    AND (
      "t23"."t0" + INTERVAL '604800' SECOND
    ) = "t75"."t0"
) AS "t76"
