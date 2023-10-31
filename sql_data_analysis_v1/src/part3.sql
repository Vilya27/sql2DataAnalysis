--1
CREATE OR REPLACE FUNCTION fnc_return_transferred_points()
    RETURNS TABLE
            (
                "Peer1"        varchar,
                "Peer2"        varchar,
                "PointsAmount" numeric
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT CASE
                   WHEN checked_nickname > checking_nickname THEN checked_nickname
                   ELSE checking_nickname
                   END,
               CASE
                   WHEN checking_nickname < checked_nickname THEN checking_nickname
                   ELSE checked_nickname
                   END,
               SUM(CASE
                       WHEN checked_nickname < checking_nickname THEN tp.transferred_points
                       ELSE tp.transferred_points * -1 END)
        FROM transferred_points tp
        GROUP BY 1, 2
        ORDER BY 1, 2;
END;
$$;

--SELECT checks.peer_nickname, p.peer_nickname FROM checks JOIN p2p p on checks.id = p.check_id
--WHERE state = 'start'

--SELECT * FROM fnc_return_transferred_points();

--2
CREATE OR REPLACE FUNCTION fnc_return_peer_task_xp()
    RETURNS TABLE
        (
        "Peer" varchar,
        "Task" varchar,
        "XP" integer
         )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
    WITH data as (
        SELECT * FROM checks JOIN xp x ON checks.id = x.check_id
    )
    SELECT peer_nickname, task_name, xp_received FROM data;
END;
$$;

-- SELECT * FROM fnc_return_peer_task_xp();

--3
CREATE OR REPLACE FUNCTION fnc_trudoholic_peers(given_date date)
    RETURNS TABLE
            (
                "Peer" varchar
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT nickname
        FROM time_tracking
        WHERE date = given_date
        EXCEPT
        SELECT nickname
        FROM time_tracking
        WHERE date = given_date
          AND state = 2;
END;
$$;

-- SELECT * FROM fnc_trudoholic_peers('2023-05-01');

--4
CREATE OR REPLACE PROCEDURE proc_changes_in_transferred_points_1(data refcursor)
    LANGUAGE plpgsql
AS
$$
BEGIN
    OPEN data FOR
        SELECT add.peer,
               CASE
                   WHEN (add.peer IS NULL) THEN -(sub.sum)
                   WHEN (sub.peer IS NULL) THEN add.sum
                   ELSE add.sum - sub.sum END AS pointschange
        FROM (SELECT checking_nickname AS peer, SUM(transferred_points.transferred_points)
              FROM transferred_points
              GROUP BY 1) add
                 FULL JOIN
             (SELECT checked_nickname AS peer, SUM(transferred_points.transferred_points)
              FROM transferred_points
              GROUP BY 1) sub
             ON add.peer = sub.peer
        ORDER BY 2 DESC, 1;
END;
$$;

-- BEGIN;
-- CALL proc_changes_in_transferred_points_1('data');
-- FETCH ALL IN "data";
-- CLOSE data;
-- COMMIT;

--5
CREATE OR REPLACE PROCEDURE proc_changes_in_transferred_points_2(data refcursor)
    LANGUAGE plpgsql
AS
$$
BEGIN
    OPEN data FOR
        WITH data AS (SELECT "Peer1" as Peer1, sum("PointsAmount") as PointsChangePlus
                      FROM fnc_return_transferred_points()
                      GROUP BY peer1)
        SELECT Peer,
               PointsChangePlus + PointsChangeMinus AS PointsChange
        FROM (SELECT COALESCE(Peer1, Peer2)         AS Peer,
                     COALESCE(PointsChangePlus, 0)  AS PointsChangePlus,
                     COALESCE(Peer2, Peer1)         AS Peer_double,
                     COALESCE(PointsChangeMinus, 0) AS PointsChangeMinus
              FROM (data
                  FULL JOIN (SELECT "Peer2" as Peer2, -sum("PointsAmount") as PointsChangeMinus
                             FROM fnc_return_transferred_points()
                             GROUP BY peer2) AS X on data.Peer1 = X.Peer2) AS Y) AS Z
        ORDER BY 2 DESC, 1;
END;
$$;

--BEGIN;
--CALL proc_changes_in_transferred_points_2('data');
--FETCH ALL IN "data";
--CLOSE data;
--COMMIT;

--6
CREATE OR REPLACE PROCEDURE proc_often_checked_task_per_day(data refcursor)
    LANGUAGE plpgsql
AS
$$
BEGIN
    OPEN data FOR 
    SELECT check_date, task_name
    FROM (
    SELECT check_date, task_name,COUNT(*), MAX(COUNT(*)) OVER (PARTITION BY check_date)
    FROM checks
    GROUP BY check_date, task_name
    ORDER BY check_date, task_name) f
    WHERE f.count = f.max;
END;
$$;

-- BEGIN;
-- CALL proc_often_checked_task_per_day('data');
-- FETCH ALL IN "data";
-- CLOSE data;
-- COMMIT;

--7
CREATE OR REPLACE FUNCTION fnc_peers_finished_block_of_tasks(block varchar)
    RETURNS TABLE ("Peer" varchar, "Day" date)
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF block = 'C' THEN
        RETURN QUERY
        SELECT peer_nickname AS "Peer", check_date AS "Day"
        FROM checks
        WHERE task_name LIKE 'C8%'
        ORDER BY check_date;
    ELSEIF block = 'CPP' THEN
        RETURN QUERY
        SELECT peer_nickname AS "Peer", check_date AS "Day"
        FROM checks
        WHERE task_name LIKE 'CPP4%'
        ORDER BY check_date;
    ELSEIF block = 'DO' THEN
        RETURN QUERY
        SELECT peer_nickname AS "Peer", check_date AS "Day"
        FROM checks
        WHERE task_name LIKE 'DO6%'
        ORDER BY check_date;
    ELSEIF block = 'SQL' THEN
        RETURN QUERY
        SELECT peer_nickname AS "Peer", check_date AS "Day"
        FROM checks
        WHERE task_name LIKE 'SQL3%'
        ORDER BY check_date;
    END IF;
END;
$$;

-- SELECT * FROM fnc_peers_finished_block_of_tasks('CPP');

--8
CREATE OR REPLACE FUNCTION fnc_recommended_peers()
    RETURNS TABLE
            (
                Peer            varchar,
                RecommendedPeer varchar
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH data AS (SELECT friend1, friend2, recommend_nickname
                      FROM (SELECT friend1, friend2
                            FROM friends
                            UNION
                            SELECT friend2, friend1
                            FROM friends) AS friendlist
                               JOIN recommendations r on r.nickname = friendlist.friend2
                      WHERE recommend_nickname <> friend1)

        SELECT DISTINCT ON (friend1) friend1 AS Peer, recommend_nickname AS RecommendedPeer
        FROM data
        GROUP BY friend1, recommend_nickname
        ORDER BY friend1, COUNT(recommend_nickname) DESC;
END;
$$;

-- SELECT * FROM fnc_recommended_peers();

--9
CREATE OR REPLACE FUNCTION fnc_procent_of_peers (block1 varchar, block2 varchar)
    RETURNS TABLE ("StartedBlock1" numeric, "StartedBlock2" numeric, "StartedBothBlocks" numeric, "DidntStartAnyBlock" numeric)
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT(SELECT round(100.00*COUNT(DISTINCT(ch.peer_nickname)) / COUNT(peers.nickname)) AS "Percent" 
        FROM peers p
        FULL JOIN checks ch ON p.nickname = ch.peer_nickname
        WHERE ch.task_name LIKE block1 || '_\_%' AND p.nickname NOT IN (SELECT DISTINCT(nickname) FROM peers p2 FULL JOIN checks ch2 ON p2.nickname = ch2.peer_nickname WHERE ch2.task_name LIKE block2 || '_\_%')) AS "StartedBlock1", 

        (SELECT round(100.00*COUNT(DISTINCT(ch.peer_nickname)) / COUNT(peers.nickname)) AS "Percent" 
        FROM peers p
        FULL JOIN checks ch ON p.nickname = ch.peer_nickname
        WHERE ch.task_name LIKE block2 || '_\_%' AND p.nickname NOT IN (SELECT DISTINCT(nickname) FROM peers p2 FULL JOIN checks ch2 ON p2.nickname = ch2.peer_nickname WHERE ch2.task_name LIKE block1 || '_\_%')) AS "StartedBlock2", 

        (SELECT round((100.00 * COUNT (DISTINCT (checks.peer_nickname)) / COUNT(peers.nickname))) AS "Percent" 
        FROM checks
        JOIN checks ch ON checks.peer_nickname = ch.peer_nickname
        WHERE checks.task_name LIKE block1 || '_\_%' AND ch.task_name  LIKE block2 || '_\_%') AS "StartedBothBlocks",

        (SELECT round(100.00 * COUNT (p.nickname) / COUNT(peers.nickname)) AS "Percent" 
        FROM peers p
        WHERE p.nickname NOT IN (SELECT peer_nickname FROM checks)) AS "DidntStartAnyBlock"
    FROM peers;
END;
$$;

-- SELECT * FROM fnc_procent_of_peers('CPP', 'C');

--10
CREATE OR REPLACE FUNCTION fnc_peers_checked_on_birthday ()
        RETURNS TABLE ("SuccessfulChecks" numeric, "UnsuccessfulChecks" numeric)
        LANGUAGE plpgsql
AS 
$$
BEGIN
        RETURN QUERY
        WITH success as (
                SELECT DISTINCT(checks.peer_nickname), state FROM checks 
                JOIN p2p on p2p.check_id = checks.id
                JOIN peers on  peers.nickname = checks.peer_nickname
                WHERE EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND p2p.state = 'success'
                ),
                failure as (
                SELECT DISTINCT(checks.peer_nickname), state FROM checks 
                JOIN p2p on p2p.check_id = checks.id
                JOIN peers on  peers.nickname = checks.peer_nickname
                WHERE EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND p2p.state = 'failure'
                ),
                data as (
                SELECT DISTINCT(checks.peer_nickname), state FROM checks 
                JOIN p2p on p2p.check_id = checks.id
                JOIN peers on  peers.nickname = checks.peer_nickname
                WHERE EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND (p2p.state = 'failure' OR p2p.state = 'success')
                )
        SELECT (
                SELECT round(100.00 * COUNT(peer_nickname) / COUNT(data.peer_nickname)) AS "Percent" 
                FROM success
                ) AS SuccessfulChecks,
                (
                SELECT round(100.00 * COUNT(peer_nickname) / COUNT(data.peer_nickname)) AS "Percent" 
                FROM failure
                ) AS UnsuccessfulChecks
        FROM data;
END;
$$;

-- SELECT * FROM fnc_peers_checked_on_birthday();



--11
DROP FUNCTION fnc_peers_finished_1_and_2_but_not_3(task1 varchar, task2 varchar, task3 varchar);
CREATE OR REPLACE FUNCTION fnc_peers_finished_1_and_2_but_not_3(task1 varchar, task2 varchar, task3 varchar)
    RETURNS TABLE(peer_name varchar)
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
    WITH data AS (SELECT *
                  FROM checks
                           JOIN xp x on checks.id = x.check_id)
    SELECT DISTINCT peer_nickname as peer_name
    FROM data
    WHERE task_name = task1 AND task_name = task2
      AND task_name != task3;
END;
$$;

-- SELECT * FROM fnc_peers_finished_1_and_2_but_not_3('C3_s21_bash_utils', 'C4_s21_math', 'C2_s21_string');
SELECT peer_nickname as peer_name
FROM (
    SELECT *
                  FROM checks
                           JOIN xp x on checks.id = x.check_id
                           ORDER BY peer_nickname) t ;
-- WHERE task_name IN ('C3_s21_bash_utils', 'C4_s21_math')
--       AND task_name NOT IN ('C2_s21_string');




--12
CREATE OR REPLACE FUNCTION fnc_recursive_amount_of_prev_tasks()
        RETURNS TABLE ("Task" varchar, "PrevCount" integer)
        LANGUAGE plpgsql
AS
$$
BEGIN 
        RETURN QUERY
        WITH RECURSIVE r AS (
        SELECT title AS Task,  0 PrevCount
        FROM tasks
        WHERE title LIKE 'C3_s21_bash_utils'
        UNION 
        SELECT t.title , PrevCount+1 
        FROM r 
        JOIN tasks t ON t.parent_task = r.task
        )
        SELECT * FROM r;
END;
$$;

-- SELECT * FROM fnc_recursive_amount_of_prev_tasks();

--13
CREATE OR REPLACE FUNCTION fnc_lucky_days_for_checks(success_checks_number int)
    RETURNS TABLE
            (
                lucky_day date
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH data AS (SELECT check_date, time, xp_received, max_xp
                      FROM checks
                               FULL JOIN xp x on checks.id = x.check_id
                               JOIN tasks t on t.title = checks.task_name
                               JOIN p2p p on checks.id = p.check_id
                      WHERE state = 'start' AND (xp_received IS NULL OR xp_received >= max_xp * 0.8))
        SELECT DISTINCT check_date
        FROM (SELECT check_date,
                     time,
                     xp_received,
                     max_xp,
                     row_number() OVER (PARTITION BY check_date, X ORDER BY time) - X AS series_length
              FROM (SELECT check_date,
                           time,
                           xp_received,
                           max_xp,
                           COUNT(CASE WHEN xp_received IS NULL THEN 1 END)
                           OVER (PARTITION BY check_date ORDER BY time) X
                    FROM data) AS Q) AS Z
        WHERE series_length >= success_checks_number
        ORDER BY 1;
END;
$$;

-- SELECT * FROM fnc_lucky_days_for_checks(2);

--14
CREATE OR REPLACE FUNCTION fnc_peer_with_max_xp()
    RETURNS TABLE
            (
                Peer varchar,
                XP   bigint
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT DISTINCT peer_nickname                                      as Peer,
                        sum(xp_received) OVER (PARTITION BY peer_nickname) as XP
        FROM checks
                 JOIN xp x on checks.id = x.check_id
        ORDER BY XP DESC
        LIMIT 1;
END;
$$;

-- SELECT * FROM fnc_peer_with_max_xp();

--15
CREATE OR REPLACE FUNCTION fnc_peers_goes_before_time(before_time time, num_times bigint)
    RETURNS TABLE
            (
                Peer varchar
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH data AS (SELECT nickname,
                             date,
                             time,
                             state,
                             COUNT(nickname) OVER (PARTITION BY nickname) AS total_entrances
                      FROM time_tracking
                      WHERE state = 1
                        AND time < before_time)
        SELECT DISTINCT nickname
        FROM data
        WHERE total_entrances >= num_times
        ORDER BY 1;
END;
$$;

-- SELECT * FROM fnc_peers_goes_before_time('10:00:00', 2);

--16
CREATE OR REPLACE FUNCTION fnc_peers_goes_out_of_campus_last_days(num_days integer, num_times bigint)
    RETURNS TABLE
            (
                Peer varchar
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH data AS (SELECT nickname,
                             date,
                             time,
                             state,
                             COUNT(nickname) OVER (PARTITION BY nickname) AS total_exits
                      FROM time_tracking
                      WHERE state = 2
                        AND date >= CURRENT_DATE - num_days)
        SELECT DISTINCT nickname
        FROM data
        WHERE total_exits > num_times
        ORDER BY 1;
END;
$$;

-- SELECT * FROM fnc_peers_goes_out_of_campus_last_days(180, 1);


--17
CREATE OR REPLACE FUNCTION fnc_early_entries()
    RETURNS TABLE
            (
                "Month"        text,
                "EarlyEntries" double precision
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        WITH data AS (SELECT TO_CHAR(date, 'Month') as month_name,
                             p.nickname,
                             birthday,
                             date,
                             time,
                             state
                      FROM time_tracking
                               JOIN peers p on p.nickname = time_tracking.nickname)
        SELECT all_months AS Month, COALESCE(Z.EarlyEntries, 0)
        FROM (SELECT DISTINCT month_name                                          AS "month",
                              CEILING(early_entries::float / total_entries * 100) AS EarlyEntries
              FROM (SELECT *,
                           COUNT(nickname) OVER (PARTITION BY month_name) AS early_entries
                    FROM (SELECT month_name,
                                 nickname,
                                 birthday,
                                 date,
                                 time,
                                 state,
                                 COUNT(nickname) OVER (PARTITION BY month_name) AS total_entries
                          FROM data


                          WHERE state = 1
                            AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM birthday)) AS X
                    WHERE time < '12:00:00') AS Y) AS Z
                 FULL JOIN (SELECT distinct to_char(dd, 'Month') as all_months
                            FROM generate_series(to_date('2023', 'YYYY'), to_date('2024', 'YYYY'),
                                                 '1 month') as dd) as ddd on ddd.all_months = Z."month"

        ORDER BY EXTRACT(MONTH FROM TO_DATE("all_months", 'Month'));
END;
$$;

-- SELECT * FROM fnc_early_entries();
