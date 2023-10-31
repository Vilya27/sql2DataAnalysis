--6
-- SELECT task_name, check_date, COUNT(task_name) AS count FROM checks
-- GROUP BY task_name, check_date
-- ORDER BY check_date, count DESC 

-- SELECT DISTINCT ON (check_date) check_date AS Day, task_name AS Task, COUNT(task_name) OVER ( PARTITION BY task_name, check_date ) as count 
-- FROM checks
-- ORDER BY check_date,task_name, count DESC

-- SELECT  DISTINCT ON (check_date,task_name) check_date AS "Day", task_name AS "Task"
-- FROM checks
-- GROUP BY task_name, check_date
-- ORDER BY check_date,task_name DESC



-- SELECT *
-- FROM 
-- (SELECT check_date, COUNT(task_name) as c, nth_value(COUNT(task_name),1) OVER (PARTITION BY check_date ORDER BY task_name) ,task_name
-- FROM checks
-- GROUP BY check_date, task_name
-- -- HAVING  COUNT(task_name) >1
-- ORDER BY check_date, task_name ) as t
-- WHERE t.count = t.nth_value

-- WITH data as (SELECT check_date, COUNT(task_name) as c, task_name
-- FROM checks
-- GROUP BY check_date, task_name
-- ORDER BY check_date, c DESC)

-- SELECT *
-- FROM data d JOIN data d1 ON d.check_date = d1.check_date 




-- WITH D AS (SELECT check_date, task_name, COUNT(task_name)
-- FROM checks
-- GROUP BY check_date, task_name
-- ORDER BY check_date,  COUNT(task_name) DESC)

-- SELECT check_date,task_name, first_value(task_name) OVER (PARTITION BY task_name)
-- FROM D 
-- ORDER BY check_date


-- SELECT check_date, COUNT(task_name) as c, nth_value(COUNT(task_name),1) OVER (PARTITION BY check_date,task_name ORDER BY (task_name)DESC) ,task_name
-- FROM checks
-- GROUP BY check_date, task_name
-- -- HAVING  COUNT(task_name) >1
-- ORDER BY check_date, c DESC


-- SELECT DISTINCT ON (check_date,task_name,count) count, check_date, task_name, currval (count)
-- SELECT case when count>2 then task_name end
-- FROM(
-- SELECT c1.check_date, c1.task_name, COUNT(c1.task_name)
-- FROM checks c1 JOIN checks c2 ON c1.id = c2.id
-- GROUP BY c1.task_name, c1.check_date
-- ORDER BY c1.check_date, c1.task_name DESC
-- ) t





-- SELECT DISTINCT on (check_date, task_name) check_date, task_name, COUNT(task_name) OVER (PARTITION BY check_date, task_name)
-- FROM checks
-- GROUP by check_date, task_name
-- HAVING COUNT(task_name) > 1)


-- SELECT  *, RANK() OVER (PARTITION by check_date order by check_date) 
-- FROM checks



-- SELECT check_date, task_name, MAX(count)
-- FROM
-- (SELECT check_date, task_name, COUNT(task_name) as count
-- FROM checks
-- GROUP BY check_date, task_name
-- ORDER BY check_date,  COUNT(task_name) DESC) t
-- GROUP BY check_date, task_name
















SELECT check_date, task_name
FROM (
SELECT check_date, task_name,COUNT(*), MAX(COUNT(*)) OVER (PARTITION BY check_date)
FROM checks
GROUP BY check_date, task_name
ORDER BY check_date, task_name) f
WHERE f.count = f.max








-- CREATE OR REPLACE FUNCTION FncMostCheckedTask()
--   RETURNS TABLE(Day DATE, Task VARCHAR)AS $$
--   WITH
--     a AS (SELECT check_date, title, COUNT(title) AS CTitle
--       FROM checks
--       JOIN tasks
--       ON checks.task_name = tasks.title
--       GROUP BY check_date, title)
--     SELECT a.check_date, title
--       FROM a
--       JOIN (SELECT check_date, MAX(CTitle) AS MaxCTitle
--       	      FROM a
-- 	    GROUP BY check_date) AS aa
--       ON a.check_date = aa.check_date AND a.CTitle = aa.MaxCtitle
--       ORDER BY check_date, title;
--   $$ LANGUAGE SQL;

-- SELECT * from FncMostCheckedTask();

--7
-- WHERE task_name LIKE 'C_\_%' - C блок заданий 
-- WHERE task_name LIKE 'CPP%' - CPP блок заданий 
-- WHERE task_name LIKE 'DO%' - DO блок заданий 
-- DROP PROCEDURE proc_peers_finished_block_of_tasks(bloc varchar);

-- CREATE OR REPLACE FUNCTION fnc_peers_finished_block_of_tasks (blok varchar)
--     RETURNS TABLE (Peer varchar, Day date)
--     -- LANGUAGE plpgsql
-- AS 
-- $$   
-- -- BEGIN
--     -- RETURN QUERY 
--     SELECT peer_nickname AS "Peer", check_date AS "Day"
--     FROM checks
--     WHERE task_name LIKE 'C8\_%' 
--     ORDER BY check_date;
-- -- END;
-- $$ LANGUAGE SQL;

-- SELECT * FROM proc_peers_finished_block_of_tasks('CPP');
-- DROP FUNCTION fnc_peers_finished_block_of_tasks (block varchar);
-- CREATE OR REPLACE FUNCTION fnc_peers_finished_block_of_tasks(block varchar)
--     RETURNS TABLE ("Peer" varchar, "Day" date)
--     LANGUAGE plpgsql
-- AS
-- $$
-- BEGIN 
--     IF block = 'C' THEN
--         RETURN QUERY
--         SELECT peer_nickname AS "Peer", check_date AS "Day"
--         FROM checks
--         WHERE task_name LIKE 'C8%'
--         ORDER BY check_date;
--     ELSEIF block = 'CPP' THEN
--         RETURN QUERY
--         SELECT peer_nickname AS "Peer", check_date AS "Day"
--         FROM checks
--         WHERE task_name LIKE 'CPP%'
--         ORDER BY check_date;
--     ELSEIF block = 'DO' THEN
--         RETURN QUERY
--         SELECT peer_nickname AS "Peer", check_date AS "Day"
--         FROM checks
--         WHERE task_name LIKE 'DO6%'
--         ORDER BY check_date;
--     ELSEIF block = 'SQL' THEN
--         RETURN QUERY
--         SELECT peer_nickname AS "Peer", check_date AS "Day"
--         FROM checks
--         WHERE task_name LIKE 'SQL3%'
--         ORDER BY check_date;
--     END IF;
-- END;
-- $$;

-- SELECT * FROM fnc_peers_finished_block_of_tasks('CPP');

-- --9
-- DROP FUNCTION fnc_procent_of_peers (block1 varchar, block2 varchar);
-- CREATE OR REPLACE FUNCTION fnc_procent_of_peers (block1 varchar, block2 varchar)
--     RETURNS TABLE ("StartedBlock1" numeric, "StartedBlock2" numeric, "StartedBothBlocks" numeric, "DidntStartAnyBlock" numeric)
--     LANGUAGE plpgsql
-- AS
-- $$
-- BEGIN
--     RETURN QUERY
--         SELECT(SELECT round(100.00*COUNT(DISTINCT(ch.peer_nickname)) / COUNT(peers.nickname)) AS "Percent" 
--         FROM peers p
--         FULL JOIN checks ch ON p.nickname = ch.peer_nickname
--         WHERE ch.task_name LIKE block1 || '_\_%' AND p.nickname NOT IN (SELECT DISTINCT(nickname) FROM peers p2 FULL JOIN checks ch2 ON p2.nickname = ch2.peer_nickname WHERE ch2.task_name LIKE block2 || '_\_%')) AS "StartedBlock1", 

--         (SELECT round(100.00*COUNT(DISTINCT(ch.peer_nickname)) / COUNT(peers.nickname)) AS "Percent" 
--         FROM peers p
--         FULL JOIN checks ch ON p.nickname = ch.peer_nickname
--         WHERE ch.task_name LIKE block2 || '_\_%' AND p.nickname NOT IN (SELECT DISTINCT(nickname) FROM peers p2 FULL JOIN checks ch2 ON p2.nickname = ch2.peer_nickname WHERE ch2.task_name LIKE block1 || '_\_%')) AS "StartedBlock2", 

--         (SELECT round((100.00 * COUNT (DISTINCT (checks.peer_nickname)) / COUNT(peers.nickname))) AS "Percent" 
--         FROM checks
--         JOIN checks ch ON checks.peer_nickname = ch.peer_nickname
--         WHERE checks.task_name LIKE block1 || '_\_%' AND ch.task_name  LIKE block2 || '_\_%') AS "StartedBothBlocks",

--         (SELECT round(100.00 * COUNT (p.nickname) / COUNT(peers.nickname)) AS "Percent" 
--         FROM peers p
--         WHERE p.nickname NOT IN (SELECT peer_nickname FROM checks)) AS "DidntStartAnyBlock"
--     FROM peers;
-- END;
-- $$;

-- SELECT * FROM fnc_procent_of_peers('CPP', 'C');


-- SELECT COUNT(DISTINCT(checks.peer_nickname)), COUNT(DISTINCT(peers.nickname))
-- WITH data as(
-- SELECT peer_nickname, task_name, nickname
-- FROM checks 
-- FULL JOIN peers ON peers.nickname = checks.peer_nickname
-- )
-- -- SELECT DISTINCT(peer_nickname) 
-- SELECT peer_nickname, task_name
-- FROM data
-- -- WHERE task_name != 'C_\_%'
-- GROUP BY peer_nickname, task_name
-- HAVING task_name NOT IN('CPP_\_%')
-- ORDER BY peer_nickname



-- SELECT 100*COUNT(DISTINCT(ch.peer_nickname)) /7
-- FROM peers p
-- FULL JOIN checks ch ON p.nickname = ch.peer_nickname
-- WHERE ch.task_name LIKE 'C_\_%' AND p.nickname NOT IN (SELECT DISTINCT(nickname) FROM peers p2 FULL JOIN checks ch2 ON p2.nickname = ch2.peer_nickname WHERE ch2.task_name LIKE 'CPP_\_%');

-- SELECT nickname FROM peers p2 FULL JOIN checks ch2 ON p2.nickname = ch2.peer_nickname WHERE ch2.task_name LIKE 'C_\_%'

-- -- SELECT ceil(100 * COUNT (DISTINCT (checks.peer_nickname)) / COUNT(checks.peer_nickname)) AS "Percent" 
-- SELECT 100*COUNT(DISTINCT(peer_nickname))/7
--         FROM checks
--         -- JOIN checks ch ON checks.peer_nickname = ch.peer_nickname
--         WHERE checks.task_name LIKE 'C_\_%' ;
-- SELECT 100*COUNT(DISTINCT(peer_nickname))/7
--         FROM checks
--         -- JOIN checks ch ON checks.peer_nickname = ch.peer_nickname
--         WHERE checks.task_name LIKE 'CPP_\_%' ;
        

-- SELECT ceil(100 * COUNT (DISTINCT (peer_nickname)) / 7) AS "Percent", COUNT (DISTINCT (peer_nickname)),
-- SELECT DISTINCT(peer_nickname), task_name
--         FROM checks
--         WHERE task_name LIKE 'C_\_%'

-- SELECT (SELECT ceil(100 * COUNT (DISTINCT (peer_nickname)) / COUNT(peers.nickname)) AS "Percent" 
--         FROM checks
--         WHERE task_name LIKE 'CPP_\_%') AS "StartedBlock1" ,
--         (SELECT ceil(100 * COUNT (DISTINCT (peer_nickname)) / COUNT(peers.nickname)) AS "Percent" 
--         FROM checks
--         WHERE task_name LIKE'C_\_%') AS "StartedBlock2",
--         (SELECT ceil(100 * COUNT (DISTINCT (checks.peer_nickname)) / COUNT(peers.nickname)) AS "Percent" 
--         FROM checks
--         JOIN checks ch ON checks.peer_nickname = ch.peer_nickname
--         WHERE checks.task_name LIKE 'CPP_\_%' AND ch.task_name  LIKE 'C_\_%') AS "StartedBothBlocks",
--         (SELECT ceil(100 * COUNT (p.nickname) / COUNT(peers.nickname)) AS "Percent" 
--         FROM peers p
--         WHERE p.nickname NOT IN (SELECT peer_nickname FROM checks)) AS "DidntStartAnyBlock"
-- FROM peers;
-- LIMIT 1


-- WITH bl1 AS (
--         SELECT ceil(100 * COUNT (DISTINCT (peer_nickname)) / 6) AS "Percent" 
--         FROM checks
--         WHERE task_name LIKE 'CPP_\_%'   
--         )
--     bl2 AS (
--         SELECT ceil(100 * COUNT (DISTINCT (peer_nickname)) / 6) AS "Percent" 
--         FROM checks
--         WHERE task_name LIKE 'CPP_\_%'
--     )
-- SELECT * FROM bl1 JOIN bl2;
-- SELECT ceil(100 * COUNT (DISTINCT (peer_nickname)) / 6) AS "Percent" 

--10

-- SELECT ceil(100 * COUNT (nickname) / 7)
-- FROM peers
-- WHERE EXISTS (SELECT 1 FROM checks JOIN p2p on p2p.check_id = checks.id
--                 WHERE peers.nickname = checks.peer_nickname AND EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND p2p.state = 'success')

-- SELECT  DISTINCT (checks.peer_nickname)
-- FROM checks JOIN p2p on p2p.check_id = checks.id
-- WHERE  p2p.state = 'success'

-- DROP FUNCTION fnc_peers_checked_on_birthday();
-- CREATE OR REPLACE FUNCTION fnc_peers_checked_on_birthday ()
--         RETURNS TABLE ("SuccessfulChecks" numeric, "UnsuccessfulChecks" numeric)
--         LANGUAGE plpgsql
-- AS 
-- $$
-- BEGIN
--         RETURN QUERY
--         WITH success as (
--                 SELECT DISTINCT(checks.peer_nickname), state FROM checks 
--                 JOIN p2p on p2p.check_id = checks.id
--                 JOIN peers on  peers.nickname = checks.peer_nickname
--                 WHERE EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND p2p.state = 'success'
--                 ),
--                 failure as (
--                 SELECT DISTINCT(checks.peer_nickname), state FROM checks 
--                 JOIN p2p on p2p.check_id = checks.id
--                 JOIN peers on  peers.nickname = checks.peer_nickname
--                 WHERE EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND p2p.state = 'failure'
--                 ),
--                 data as (
--                 SELECT DISTINCT(checks.peer_nickname), state FROM checks 
--                 JOIN p2p on p2p.check_id = checks.id
--                 JOIN peers on  peers.nickname = checks.peer_nickname
--                 WHERE EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND (p2p.state = 'failure' OR p2p.state = 'success')
--                 )
--         SELECT (
--                 SELECT round(100.00 * COUNT(peer_nickname) / COUNT(data.peer_nickname)) AS "Percent" 
--                 FROM success
--                 ) AS SuccessfulChecks,
--                 (
--                 SELECT round(100.00 * COUNT(peer_nickname) / COUNT(data.peer_nickname)) AS "Percent" 
--                 FROM failure
--                 ) AS UnsuccessfulChecks
--         FROM data;
-- END;
-- $$;

-- SELECT * FROM fnc_peers_checked_on_birthday();


-- SELECT ceil(100 * COUNT (nickname) / COUNT(nickname)) AS success,  COUNT(nickname)
--                 FROM peers
--                 WHERE EXISTS (SELECT 1 FROM checks JOIN p2p on p2p.check_id = checks.id
--                                 WHERE peers.nickname = checks.peer_nickname AND EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND p2p.state = 'success');
-- SELECT ceil(100 * COUNT (nickname) / COUNT(nickname)) AS fail,  COUNT(nickname)
--                 FROM peers
--                 WHERE EXISTS (SELECT 1 FROM checks JOIN p2p on p2p.check_id = checks.id
--                                 WHERE peers.nickname = checks.peer_nickname AND EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND p2p.state = 'failure');

-- WITH success as (
--         SELECT DISTINCT(checks.peer_nickname), state FROM checks 
--         JOIN p2p on p2p.check_id = checks.id
--         JOIN peers on  peers.nickname = checks.peer_nickname
--         WHERE EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND p2p.state = 'success'
--         ),
--         failure as (
--         SELECT DISTINCT(checks.peer_nickname), state FROM checks 
--         JOIN p2p on p2p.check_id = checks.id
--         JOIN peers on  peers.nickname = checks.peer_nickname
--         WHERE EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND p2p.state = 'failure'
--         ),
--         data as (
--         SELECT DISTINCT(checks.peer_nickname), state FROM checks 
--         JOIN p2p on p2p.check_id = checks.id
--         JOIN peers on  peers.nickname = checks.peer_nickname
--         WHERE EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND (p2p.state = 'failure' OR p2p.state = 'success')
--         )
-- SELECT (
--         SELECT 100 * COUNT(peer_nickname) / COUNT(data.peer_nickname)
--         FROM success
--         ) AS SuccessfulChecks,
--         (
--         SELECT 100 * COUNT(peer_nickname) / COUNT(data.peer_nickname)
--         FROM failure
--         ) AS UnsuccessfulChecks
-- FROM data;
                



-- SELECT ceiling(100 * COUNT (nickname) / 6), COUNT (nickname)
--                 FROM peers
--                 WHERE EXISTS (SELECT 1 FROM checks JOIN p2p on p2p.check_id = checks.id
--                                 WHERE peers.nickname = checks.peer_nickname AND EXTRACT(DAY FROM peers.birthday) = EXTRACT(DAY FROM checks.check_date) AND p2p.state = 'success')       
                
--12

-- SELECT title, COUNT(parent_task)
-- SELECT title, SUBSTRING(parent_task from '%#"___#"%' for '#')
-- FROM tasks
-- GROUP BY title
-- HAVING

-- SELECT t1.title, t1.parent_task
-- FROM tasks t1
-- JOIN tasks t2 ON t1.parent_task = t2.parent_task
-- DROP FUNCTION fnc_recursive_amount_of_prev_tasks();
-- CREATE OR REPLACE FUNCTION fnc_recursive_amount_of_prev_tasks()
--         RETURNS TABLE ("Task" varchar, "PrevCount" integer)
--         LANGUAGE plpgsql
-- AS
-- $$
-- BEGIN 
--         RETURN QUERY
--         WITH RECURSIVE r AS (
--         SELECT title AS Task,  0 PrevCount
--         FROM tasks
--         WHERE title LIKE 'C3_s21_bash_utils'
--         UNION 
--         SELECT t.title , PrevCount+1 
--         FROM r 
--         JOIN tasks t ON t.parent_task = r.task
--         )
--         SELECT * FROM r;
-- END;
-- $$;

-- SELECT * FROM fnc_recursive_amount_of_prev_tasks();

