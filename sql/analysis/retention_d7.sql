-- 7-day retention: share of the users first seen on the cohort day who are
-- active again 7 days later. The cohort is the first loaded day; with fewer
-- than 8 days loaded, retained_users is 0.
WITH params AS (
    SELECT min(event_date) AS cohort_date FROM silver.events
),
cohort AS (
    SELECT user_id
    FROM silver.events
    GROUP BY user_id
    HAVING min(event_date) = (SELECT cohort_date FROM params)
),
retained AS (
    SELECT DISTINCT e.user_id
    FROM silver.events e
    JOIN cohort c USING (user_id)
    WHERE e.event_date = (SELECT cohort_date FROM params) + 7
)
SELECT (SELECT cohort_date FROM params) AS cohort_date,
       (SELECT count(*) FROM cohort) AS cohort_users,
       (SELECT count(*) FROM retained) AS retained_users,
       -- ::float avoids integer division; NULLIF avoids dividing by 0.
       (SELECT count(*) FROM retained)::float
           / NULLIF((SELECT count(*) FROM cohort), 0) AS d7_retention;
