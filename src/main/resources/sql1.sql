  with src_data
    AS (
         SELECT CustomerId                                  customer_id
              , PhoneNumber                                 phone_number
              , CallDuration                                call_duration
              , (
                  CAST(SUBSTR(CallDuration, 1, 2) AS INT) * 60 * 60
                + CAST(SUBSTR(CallDuration, 4, 2) AS INT) * 60
                + CAST(SUBSTR(CallDuration, 7, 2) AS INT)
                )                                           duration_ss
           FROM phone_log
       )
     , src_call_cost
    AS (
         SELECT customer_id
              , phone_number
              , duration_ss/60                              duration_mm
              , CASE WHEN (duration_ss/60) < 3 THEN duration_ss * 0.05
                     ELSE duration_ss * 0.03
                END                                         call_cost
           FROM src_data
       )
     , src_call_cost_total
    AS (
         SELECT customer_id
              , phone_number
              , SUM(call_cost)                              total_call_cost
           FROM src_call_cost
          GROUP BY
                customer_id
              , phone_number
       )
     , src_call_cost_rnk
    AS (
         SELECT customer_id
              , phone_number
              , total_call_cost
              , DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY total_call_cost DESC) rnk
           FROM src_call_cost_total
       )
  SELECT customer_id
       , SUM(total_call_cost)                               customer_call_cost
    FROM src_call_cost_rnk
   WHERE rnk > 1
   GROUP BY
         customer_id
   ORDER BY
         customer_id
