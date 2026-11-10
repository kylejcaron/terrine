WITH "t18" AS (
  SELECT
    "t17"."unit",
    "t17"."anchor_id",
    "t17"."__terrine_w0_s",
    "t17"."__terrine_w1_e",
    "t17"."__terrine_w5_of",
    "t17"."__terrine_w6_ou",
    (
      "t17"."t0" IS NOT NULL
    )
    AND NOT (
      (
        "t17"."__terrine_w0_s" = TIMEZONE('UTC', CAST(CAST(TIMEZONE('UTC', "t17"."__terrine_w0_s") AS DATE) AS TIMESTAMP))
      )
      AND (
        "t17"."__terrine_w1_e" = TIMEZONE('UTC', CAST(CAST(TIMEZONE('UTC', "t17"."__terrine_w1_e") AS DATE) AS TIMESTAMP))
      )
    ) AS "__terrine_w2_u",
    (
      "t17"."__terrine_w0_s" >= MAKE_TIMESTAMPTZ(2024, 12, 22, 0, 0, 0.0, 'UTC')
    )
    AND (
      "t17"."__terrine_w1_e" <= MAKE_TIMESTAMPTZ(2025, 2, 3, 0, 0, 0.0, 'UTC')
    ) AS "__terrine_w4_sc",
    (
      (
        "t17"."_observed_from" IS NULL
      )
      OR (
        "t17"."__terrine_w0_s" >= "t17"."_observed_from"
      )
    )
    AND (
      (
        "t17"."_observed_until" IS NULL
      )
      OR (
        "t17"."__terrine_w1_e" <= "t17"."_observed_until"
      )
    ) AS "__terrine_w7_o"
  FROM (
    SELECT
      "t15"."unit",
      "t15"."anchor_id",
      "t15"."t0",
      "t15"."_observed_from",
      "t15"."_observed_until",
      "t15"."t0" + INTERVAL '-604800' SECOND AS "__terrine_w0_s",
      "t15"."t0" AS "__terrine_w1_e",
      GREATEST(
        MAKE_TIMESTAMPTZ(2024, 12, 22, 0, 0, 0.0, 'UTC'),
        COALESCE("t15"."_observed_from", MAKE_TIMESTAMPTZ(2024, 12, 22, 0, 0, 0.0, 'UTC'))
      ) AS "__terrine_w5_of",
      LEAST(
        MAKE_TIMESTAMPTZ(2025, 2, 3, 0, 0, 0.0, 'UTC'),
        COALESCE("t15"."_observed_until", MAKE_TIMESTAMPTZ(2025, 2, 3, 0, 0, 0.0, 'UTC'))
      ) AS "__terrine_w6_ou"
    FROM (
      SELECT
        "t5"."unit",
        "t13"."anchor_id",
        "t13"."t0",
        "t5"."observed_from_utc" AS "_observed_from",
        "t5"."observed_until_utc" AS "_observed_until"
      FROM (
        SELECT
          "t0"."unit_id" AS "unit",
          CAST("t0"."observed_from" AS TIMESTAMPTZ) AS "observed_from_utc",
          CAST("t0"."observed_until" AS TIMESTAMPTZ) AS "observed_until_utc"
        FROM "users" AS "t0"
      ) AS "t5"
      CROSS JOIN (
        SELECT
          *
        FROM (
          SELECT
            'schedule:' || CAST("t7"."ordinal" AS TEXT) AS "anchor_id",
            MAKE_TIMESTAMPTZ(2025, 1, 8, 0, 0, 0.0, 'UTC') + TO_SECONDS("t7"."ordinal" * 604800) AS "t0"
          FROM (
            SELECT
              CAST(UNNEST(RANGE(0, 3, 1)) AS BIGINT) AS "ordinal"
          ) AS "t7"
        ) AS "t9"
        WHERE
          "t9"."t0" < MAKE_TIMESTAMPTZ(2025, 1, 29, 0, 0, 0.0, 'UTC')
      ) AS "t13"
    ) AS "t15"
  ) AS "t17"
), "t29" AS (
  SELECT
    "t28"."_unit",
    "t28"."value",
    TIMEZONE(
      'UTC',
      CAST(CAST(TIMEZONE('UTC', "t28"."__terrine_w18_dt") AS DATE) AS TIMESTAMP) - INTERVAL '0' MINUTE
    ) AS "__terrine_w10_b"
  FROM (
    SELECT
      "t27"."_unit",
      "t27"."value",
      CASE
        WHEN COALESCE(
          (
            "t27"."_timestamp" >= CAST("t27"."__terrine_w16_ss" AS TIMESTAMPTZ)
          )
          AND (
            "t27"."_timestamp" < CAST("t27"."__terrine_w17_se" AS TIMESTAMPTZ)
          ),
          FALSE
        )
        THEN "t27"."_timestamp"
        ELSE CAST(NULL AS TIMESTAMPTZ)
      END AS "__terrine_w18_dt"
    FROM (
      SELECT
        "t4"."_timestamp",
        "t4"."_unit",
        "t4"."value",
        "t26"."__terrine_w16_ss",
        "t26"."__terrine_w17_se"
      FROM (
        SELECT
          "t1"."event_time" AS "_timestamp",
          "t1"."unit_id" AS "_unit",
          "t1"."value"
        FROM "events" AS "t1"
      ) AS "t4"
      LEFT OUTER JOIN (
        SELECT
          "t24"."__terrine_w15_su",
          MIN("t24"."__terrine_w16_ss") AS "__terrine_w16_ss",
          MAX("t24"."__terrine_w17_se") AS "__terrine_w17_se"
        FROM (
          SELECT
            "t19"."unit" AS "__terrine_w15_su",
            LEAST(
              CASE
                WHEN COALESCE("t19"."__terrine_w4_sc" AND "t19"."__terrine_w7_o", FALSE)
                AND NOT (
                  COALESCE(COALESCE("t19"."__terrine_w2_u", TRUE), TRUE)
                )
                THEN "t19"."__terrine_w0_s"
                ELSE CAST(NULL AS TIMESTAMPTZ)
              END
            ) AS "__terrine_w16_ss",
            GREATEST(
              CASE
                WHEN COALESCE("t19"."__terrine_w4_sc" AND "t19"."__terrine_w7_o", FALSE)
                AND NOT (
                  COALESCE(COALESCE("t19"."__terrine_w2_u", TRUE), TRUE)
                )
                THEN "t19"."__terrine_w1_e"
                ELSE CAST(NULL AS TIMESTAMPTZ)
              END
            ) AS "__terrine_w17_se"
          FROM "t18" AS "t19"
        ) AS "t24"
        GROUP BY
          1
      ) AS "t26"
        ON "t4"."_unit" = "t26"."__terrine_w15_su"
    ) AS "t27"
  ) AS "t28"
), "t53" AS (
  SELECT
    "t52"."unit",
    "t52"."t0",
    "t52"."_state_period",
    "t52"."__terrine_value_0_observed_from",
    "t52"."__terrine_value_0_observed_until",
    COALESCE("t52"."_state_period__complete", FALSE) AS "__terrine_state_complete",
    "t52"."_state_period__reason" AS "__terrine_state_reason"
  FROM (
    SELECT
      "t5"."unit",
      "t13"."t0",
      "t51"."__terrine_value_0" AS "_state_period",
      "t51"."__terrine_value_0_complete" AS "_state_period__complete",
      "t51"."__terrine_value_0_reason" AS "_state_period__reason",
      "t51"."__terrine_value_0_observed_from",
      "t51"."__terrine_value_0_observed_until"
    FROM (
      SELECT
        "t0"."unit_id" AS "unit",
        CAST("t0"."observed_from" AS TIMESTAMPTZ) AS "observed_from_utc",
        CAST("t0"."observed_until" AS TIMESTAMPTZ) AS "observed_until_utc"
      FROM "users" AS "t0"
    ) AS "t5"
    CROSS JOIN (
      SELECT
        *
      FROM (
        SELECT
          'schedule:' || CAST("t7"."ordinal" AS TEXT) AS "anchor_id",
          MAKE_TIMESTAMPTZ(2025, 1, 8, 0, 0, 0.0, 'UTC') + TO_SECONDS("t7"."ordinal" * 604800) AS "t0"
        FROM (
          SELECT
            CAST(UNNEST(RANGE(0, 3, 1)) AS BIGINT) AS "ordinal"
        ) AS "t7"
      ) AS "t9"
      WHERE
        "t9"."t0" < MAKE_TIMESTAMPTZ(2025, 1, 29, 0, 0, 0.0, 'UTC')
    ) AS "t13"
    LEFT OUTER JOIN (
      SELECT
        "t49"."unit" AS "__terrine_value_0_key_unit",
        "t49"."anchor_id" AS "__terrine_value_0_key_anchor_id",
        CASE
          WHEN "t49"."__terrine_w33_r" IS NULL
          THEN "t49"."__terrine_w35_eo"
          ELSE CAST(NULL AS DECIMAL(38, 0))
        END AS "__terrine_value_0",
        "t49"."__terrine_w5_of" AS "__terrine_value_0_observed_from",
        "t49"."__terrine_w6_ou" AS "__terrine_value_0_observed_until",
        "t49"."__terrine_w8_c" AS "__terrine_value_0_complete",
        "t49"."__terrine_w33_r" AS "__terrine_value_0_reason"
      FROM (
        SELECT
          "t48"."unit",
          "t48"."anchor_id",
          "t48"."__terrine_w5_of",
          "t48"."__terrine_w6_ou",
          "t48"."__terrine_w8_c",
          TRY_CAST(CAST(COALESCE("t48"."__terrine_w29_sv", CAST('0' AS BIGNUM)) AS TEXT) AS DECIMAL(38, 0)) AS "__terrine_w35_eo",
          CASE
            WHEN "t48"."__terrine_w8_c"
            THEN CASE
              WHEN (
                COALESCE("t48"."__terrine_w27_ec", CAST('0' AS INT128)) > CAST('0' AS INT128)
              )
              AND (
                COALESCE("t48"."__terrine_w28_nc", CAST('0' AS INT128)) = CAST('0' AS INT128)
              )
              THEN 'all_values_null'
              ELSE CAST(NULL AS TEXT)
            END
            ELSE "t48"."__terrine_w9_ur"
          END AS "__terrine_w33_r"
        FROM (
          SELECT
            "t47".*
          FROM (
            SELECT
              "t23"."unit",
              "t23"."anchor_id",
              "t23"."__terrine_w5_of",
              "t23"."__terrine_w6_ou",
              "t23"."__terrine_w8_c",
              "t23"."__terrine_w9_ur",
              "t46"."__terrine_w27_ec",
              "t46"."__terrine_w28_nc",
              "t46"."__terrine_w29_sv"
            FROM (
              SELECT
                "t19"."unit",
                "t19"."anchor_id",
                "t19"."__terrine_w0_s",
                "t19"."__terrine_w1_e",
                "t19"."__terrine_w5_of",
                "t19"."__terrine_w6_ou",
                COALESCE("t19"."__terrine_w2_u", TRUE) AS "__terrine_w3_c",
                "t19"."__terrine_w4_sc" AND "t19"."__terrine_w7_o" AS "__terrine_w8_c",
                CASE
                  WHEN NOT (
                    "t19"."__terrine_w4_sc"
                  )
                  THEN 'incomplete_coverage'
                  ELSE CASE
                    WHEN NOT (
                      "t19"."__terrine_w7_o"
                    )
                    THEN 'incomplete_observation'
                    ELSE CAST(NULL AS TEXT)
                  END
                END AS "__terrine_w9_ur"
              FROM "t18" AS "t19"
            ) AS "t23"
            LEFT OUTER JOIN (
              SELECT
                "t44"."__terrine_w13_u",
                "t44"."__terrine_w14_ai",
                SUM("t44"."__terrine_w27_ec") AS "__terrine_w27_ec",
                SUM("t44"."__terrine_w28_nc") AS "__terrine_w28_nc",
                SUM("t44"."__terrine_w29_sv") AS "__terrine_w29_sv"
              FROM (
                SELECT
                  "t43"."__terrine_w13_u",
                  "t43"."__terrine_w14_ai",
                  CASE
                    WHEN "t43"."__terrine_w26_c"
                    THEN "t43"."__terrine_w23_ec"
                    ELSE CAST(NULL AS INT128)
                  END AS "__terrine_w27_ec",
                  CASE
                    WHEN "t43"."__terrine_w26_c"
                    THEN "t43"."__terrine_w24_nc"
                    ELSE CAST(NULL AS INT128)
                  END AS "__terrine_w28_nc",
                  CASE
                    WHEN "t43"."__terrine_w26_c"
                    THEN "t43"."__terrine_w25_sv"
                    ELSE CAST(NULL AS BIGNUM)
                  END AS "__terrine_w29_sv"
                FROM (
                  SELECT
                    "t42"."__terrine_w23_ec",
                    "t42"."__terrine_w24_nc",
                    "t42"."__terrine_w25_sv",
                    "t42"."__terrine_w13_u",
                    "t42"."__terrine_w14_ai",
                    (
                      FALSE OR "t42"."__terrine_w8_c"
                    )
                    AND (
                      "t42"."__terrine_w12_sb" >= "t42"."__terrine_w0_s"
                    )
                    AND (
                      "t42"."__terrine_w12_sb" < "t42"."__terrine_w1_e"
                    ) AS "__terrine_w26_c"
                  FROM (
                    SELECT
                      "t41"."__terrine_w12_sb",
                      "t41"."__terrine_w23_ec",
                      "t41"."__terrine_w24_nc",
                      "t41"."__terrine_w25_sv",
                      "t21"."__terrine_w13_u",
                      "t21"."__terrine_w14_ai",
                      "t21"."__terrine_w8_c",
                      "t21"."__terrine_w0_s",
                      "t21"."__terrine_w1_e"
                    FROM (
                      SELECT
                        "t39"."__terrine_w11_su",
                        "t39"."__terrine_w12_sb",
                        SUM("t39"."__terrine_w23_ec") AS "__terrine_w23_ec",
                        SUM("t39"."__terrine_w24_nc") AS "__terrine_w24_nc",
                        SUM("t39"."__terrine_w25_sv") AS "__terrine_w25_sv"
                      FROM (
                        SELECT
                          "t38"."__terrine_w11_su",
                          "t38"."__terrine_w12_sb",
                          CASE WHEN "t38"."__terrine_w22_s" THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END AS "__terrine_w23_ec",
                          CASE
                            WHEN "t38"."__terrine_w22_s" AND (
                              "t38"."value" IS NOT NULL
                            )
                            THEN CAST(1 AS BIGINT)
                            ELSE CAST(0 AS BIGINT)
                          END AS "__terrine_w24_nc",
                          CASE
                            WHEN "t38"."__terrine_w22_s"
                            THEN CAST(CAST("t38"."value" AS TEXT) AS BIGNUM)
                            ELSE CAST(NULL AS BIGNUM)
                          END AS "__terrine_w25_sv"
                        FROM (
                          SELECT
                            "t37"."value",
                            "t37"."__terrine_w11_su",
                            "t37"."__terrine_w12_sb",
                            COALESCE("t37"."__terrine_w19_c", FALSE) AND COALESCE(TRUE, FALSE) AS "__terrine_w22_s"
                          FROM (
                            SELECT
                              "t31"."value",
                              "t36"."__terrine_w11_su",
                              "t36"."__terrine_w12_sb",
                              "t36"."__terrine_w19_c"
                            FROM "t29" AS "t31"
                            INNER JOIN (
                              SELECT
                                "t34"."__terrine_w11_su",
                                "t34"."__terrine_w12_sb",
                                BOOL_OR(CAST("t34"."__terrine_w19_c" AS BOOLEAN)) AS "__terrine_w19_c"
                              FROM (
                                SELECT
                                  "t33"."__terrine_w11_su",
                                  "t33"."__terrine_w12_sb",
                                  FALSE
                                  OR (
                                    (
                                      FALSE OR "t21"."__terrine_w8_c"
                                    )
                                    AND (
                                      "t33"."__terrine_w12_sb" >= "t21"."__terrine_w0_s"
                                    )
                                    AND (
                                      "t33"."__terrine_w12_sb" < "t21"."__terrine_w1_e"
                                    )
                                  ) AS "__terrine_w19_c"
                                FROM (
                                  SELECT DISTINCT
                                    "t30"."_unit" AS "__terrine_w11_su",
                                    "t30"."__terrine_w10_b" AS "__terrine_w12_sb"
                                  FROM "t29" AS "t30"
                                ) AS "t33"
                                INNER JOIN (
                                  SELECT
                                    "t19"."unit" AS "__terrine_w13_u",
                                    "t19"."anchor_id" AS "__terrine_w14_ai",
                                    "t19"."__terrine_w4_sc" AND "t19"."__terrine_w7_o" AS "__terrine_w8_c",
                                    "t19"."__terrine_w0_s",
                                    "t19"."__terrine_w1_e"
                                  FROM "t18" AS "t19"
                                ) AS "t21"
                                  ON "t33"."__terrine_w11_su" = "t21"."__terrine_w13_u"
                                  AND (
                                    FALSE
                                    OR (
                                      (
                                        FALSE OR "t21"."__terrine_w8_c"
                                      )
                                      AND (
                                        "t33"."__terrine_w12_sb" >= "t21"."__terrine_w0_s"
                                      )
                                      AND (
                                        "t33"."__terrine_w12_sb" < "t21"."__terrine_w1_e"
                                      )
                                    )
                                  )
                              ) AS "t34"
                              GROUP BY
                                1,
                                2
                            ) AS "t36"
                              ON "t31"."_unit" = "t36"."__terrine_w11_su"
                              AND "t31"."__terrine_w10_b" = "t36"."__terrine_w12_sb"
                          ) AS "t37"
                        ) AS "t38"
                      ) AS "t39"
                      GROUP BY
                        1,
                        2
                    ) AS "t41"
                    INNER JOIN (
                      SELECT
                        "t19"."unit" AS "__terrine_w13_u",
                        "t19"."anchor_id" AS "__terrine_w14_ai",
                        "t19"."__terrine_w4_sc" AND "t19"."__terrine_w7_o" AS "__terrine_w8_c",
                        "t19"."__terrine_w0_s",
                        "t19"."__terrine_w1_e"
                      FROM "t18" AS "t19"
                    ) AS "t21"
                      ON "t41"."__terrine_w11_su" = "t21"."__terrine_w13_u"
                      AND (
                        FALSE
                        OR (
                          (
                            FALSE OR "t21"."__terrine_w8_c"
                          )
                          AND (
                            "t41"."__terrine_w12_sb" >= "t21"."__terrine_w0_s"
                          )
                          AND (
                            "t41"."__terrine_w12_sb" < "t21"."__terrine_w1_e"
                          )
                        )
                      )
                  ) AS "t42"
                ) AS "t43"
              ) AS "t44"
              GROUP BY
                1,
                2
            ) AS "t46"
              ON "t23"."unit" = "t46"."__terrine_w13_u"
              AND "t23"."anchor_id" = "t46"."__terrine_w14_ai"
          ) AS "t47"
        ) AS "t48"
      ) AS "t49"
    ) AS "t51"
      ON "t5"."unit" IS NOT DISTINCT FROM "t51"."__terrine_value_0_key_unit"
      AND "t13"."anchor_id" IS NOT DISTINCT FROM "t51"."__terrine_value_0_key_anchor_id"
  ) AS "t52"
)
SELECT
  "t57"."unit",
  "t57"."anchor_id",
  "t57"."t0",
  "t57"."from_time",
  "t57"."to_time",
  "t57"."scheduled_to",
  "t57"."period_index",
  "t57"."gap",
  "t57"."from_state",
  "t57"."from_state_complete" AS "from_state__complete",
  "t57"."from_state_reason" AS "from_state__reason",
  "t57"."to_state",
  "t57"."to_state_complete" AS "to_state__complete",
  "t57"."to_state_reason" AS "to_state__reason",
  "t57"."terminal",
  "t57"."from_state_observed_from",
  "t57"."from_state_observed_until",
  "t57"."to_state_observed_from",
  "t57"."to_state_observed_until",
  MAKE_TIMESTAMPTZ(2025, 1, 8, 0, 0, 0.0, 'UTC') AS "__terrine_panel_start"
FROM (
  SELECT
    "t16"."unit",
    "t16"."anchor_id",
    "t16"."t0",
    "t16"."t0" AS "from_time",
    "t16"."t0" + INTERVAL '604800' SECOND AS "to_time",
    "t16"."t0" + INTERVAL '604800' SECOND AS "scheduled_to",
    CAST(REGEXP_REPLACE("t16"."anchor_id", '^schedule:', '', 'g') AS BIGINT) AS "period_index",
    604800000000 AS "gap",
    CASE
      WHEN COALESCE("t55"."__terrine_state_complete", FALSE)
      THEN CASE
        WHEN "t55"."_state_period" <= CAST('0' AS DECIMAL(1, 0))
        THEN 'low'
        WHEN "t55"."_state_period" > CAST('0' AS DECIMAL(1, 0))
        THEN 'high'
        ELSE CAST(NULL AS TEXT)
      END
      ELSE CAST(NULL AS TEXT)
    END AS "from_state",
    CASE
      WHEN COALESCE("t56"."__terrine_state_complete", FALSE)
      THEN CASE
        WHEN "t56"."_state_period" <= CAST('0' AS DECIMAL(1, 0))
        THEN 'low'
        WHEN "t56"."_state_period" > CAST('0' AS DECIMAL(1, 0))
        THEN 'high'
        ELSE CAST(NULL AS TEXT)
      END
      ELSE CAST(NULL AS TEXT)
    END AS "to_state",
    FALSE AS "terminal",
    COALESCE("t55"."__terrine_state_complete", FALSE)
    AND (
      CASE
        WHEN "t55"."_state_period" <= CAST('0' AS DECIMAL(1, 0))
        THEN 'low'
        WHEN "t55"."_state_period" > CAST('0' AS DECIMAL(1, 0))
        THEN 'high'
        ELSE CAST(NULL AS TEXT)
      END IS NOT NULL
    ) AS "from_state_complete",
    CASE
      WHEN NOT (
        COALESCE("t55"."__terrine_state_complete", FALSE)
      )
      THEN COALESCE("t55"."__terrine_state_reason", 'incomplete_observation')
      WHEN COALESCE("t55"."__terrine_state_complete", FALSE)
      AND (
        "t55"."_state_period" IS NULL
      )
      THEN COALESCE("t55"."__terrine_state_reason", 'state_value_null')
      ELSE CAST(NULL AS TEXT)
    END AS "from_state_reason",
    COALESCE("t56"."__terrine_state_complete", FALSE)
    AND (
      CASE
        WHEN "t56"."_state_period" <= CAST('0' AS DECIMAL(1, 0))
        THEN 'low'
        WHEN "t56"."_state_period" > CAST('0' AS DECIMAL(1, 0))
        THEN 'high'
        ELSE CAST(NULL AS TEXT)
      END IS NOT NULL
    ) AS "to_state_complete",
    CASE
      WHEN NOT (
        COALESCE("t56"."__terrine_state_complete", FALSE)
      )
      THEN COALESCE("t56"."__terrine_state_reason", 'incomplete_observation')
      WHEN COALESCE("t56"."__terrine_state_complete", FALSE)
      AND (
        "t56"."_state_period" IS NULL
      )
      THEN COALESCE("t56"."__terrine_state_reason", 'state_value_null')
      ELSE CAST(NULL AS TEXT)
    END AS "to_state_reason",
    "t55"."__terrine_value_0_observed_from" AS "from_state_observed_from",
    "t55"."__terrine_value_0_observed_until" AS "from_state_observed_until",
    "t56"."__terrine_value_0_observed_from" AS "to_state_observed_from",
    "t56"."__terrine_value_0_observed_until" AS "to_state_observed_until"
  FROM (
    SELECT
      "t5"."unit",
      "t12"."anchor_id",
      "t12"."t0"
    FROM (
      SELECT
        "t0"."unit_id" AS "unit",
        CAST("t0"."observed_from" AS TIMESTAMPTZ) AS "observed_from_utc",
        CAST("t0"."observed_until" AS TIMESTAMPTZ) AS "observed_until_utc"
      FROM "users" AS "t0"
    ) AS "t5"
    CROSS JOIN (
      SELECT
        *
      FROM (
        SELECT
          'schedule:' || CAST("t6"."ordinal" AS TEXT) AS "anchor_id",
          MAKE_TIMESTAMPTZ(2025, 1, 8, 0, 0, 0.0, 'UTC') + TO_SECONDS("t6"."ordinal" * 604800) AS "t0"
        FROM (
          SELECT
            CAST(UNNEST(RANGE(0, 2, 1)) AS BIGINT) AS "ordinal"
        ) AS "t6"
      ) AS "t8"
      WHERE
        "t8"."t0" < MAKE_TIMESTAMPTZ(2025, 1, 22, 0, 0, 0.0, 'UTC')
    ) AS "t12"
  ) AS "t16"
  LEFT OUTER JOIN "t53" AS "t55"
    ON "t16"."unit" = "t55"."unit" AND "t16"."t0" = "t55"."t0"
  LEFT OUTER JOIN "t53" AS "t56"
    ON "t16"."unit" = "t56"."unit"
    AND (
      "t16"."t0" + INTERVAL '604800' SECOND
    ) = "t56"."t0"
) AS "t57"
