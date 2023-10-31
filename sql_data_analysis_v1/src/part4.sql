-- CREATE DATABASE part4;
--go to connection properties in DataGrip and choose database part4


DROP TABLE IF EXISTS  TableName_1, TableName_2, checks, friends, p2p, peers, recommendations, tasks, time_tracking, transferred_points, verter, XP cascade;
DROP TYPE IF EXISTS check_status CASCADE;
DROP PROCEDURE IF EXISTS csv_import(filepath text, delimiter char) CASCADE;
DROP PROCEDURE IF EXISTS csv_export(filepath text, delimiter char) CASCADE;

CREATE TABLE if NOT EXISTS TableName_1
( Field_1 varchar NOT NULL PRIMARY KEY,
  Field_2 date NOT NULL
  );

CREATE TABLE if NOT EXISTS TableName_2
( Field_1 varchar NOT NULL PRIMARY KEY,
  Field_2 date NOT NULL
  );

CREATE TABLE if NOT EXISTS peers
( nickname varchar NOT NULL PRIMARY KEY,
  birthday date NOT NULL
  );

CREATE TABLE if NOT EXISTS tasks
( title varchar NOT NULL PRIMARY KEY,
  parent_task varchar NULL REFERENCES tasks (title),
  max_XP integer NOT NULL
  );

CREATE TABLE if NOT EXISTS checks
( id bigint primary key,
  peer_nickname varchar NOT NULL,
  task_name varchar NOT NULL,
  check_date date NOT NULL,
  constraint fk_peers_id foreign key (peer_nickname) references peers(nickname),
  constraint fk_task_name foreign key (task_name) references tasks(title)
  );

CREATE TYPE check_status AS ENUM ('start', 'success', 'failure');

CREATE TABLE if NOT EXISTS P2P
( id bigint primary key,
  check_id bigint NOT NULL,
  peer_nickname varchar NOT NULL,
  state check_status NOT NULL,
  time time NOT NULL,
  constraint fk_check_id foreign key (check_id) references checks(id),
  constraint fk_peer_nickname foreign key (peer_nickname) references peers(nickname),
  UNIQUE (check_id, peer_nickname, state)
  );

CREATE TABLE if NOT EXISTS verter
( id bigint primary key,
  check_id bigint NOT NULL,
  state check_status NOT NULL,
  time time NOT NULL,
  constraint fk_check_id foreign key (check_id) references checks(id)
  );

CREATE TABLE if NOT EXISTS transferred_points
( id bigint primary key,
  checking_nickname varchar NOT NULL,
  checked_nickname varchar NOT NULL,
  transferred_points bigint NOT NULL,
  constraint fk_checking_nickname foreign key (checking_nickname) references peers(nickname),
  constraint fk_checked_nickname foreign key (checked_nickname) references peers(nickname)
  );

CREATE TABLE if NOT EXISTS friends
( id bigint primary key,
  friend1 varchar NOT NULL,
  friend2 varchar NOT NULL,
  constraint fk_friend1 foreign key (friend1) references peers(nickname),
  constraint fk_friend2 foreign key (friend2) references peers(nickname)
  );

CREATE TABLE if NOT EXISTS recommendations
( id bigint primary key,
  nickname varchar NOT NULL,
  recommend_nickname varchar NOT NULL,
  constraint fk_nickname foreign key (nickname) references peers(nickname),
  constraint fk_recommend_nickname foreign key (recommend_nickname) references peers(nickname)
  );

CREATE TABLE if NOT EXISTS XP
( id bigint primary key,
  check_id bigint NOT NULL,
  xp_received integer,
  constraint fk_check_id foreign key (check_id) references checks(id)
  );

CREATE TABLE if NOT EXISTS time_tracking
( id bigint primary key,
  nickname varchar NOT NULL,
  date date NOT NULL,
  time time NOT NULL,
  state integer NOT NULL,
  constraint fk_nickname foreign key (nickname) references peers(nickname),
  constraint ch_state check (state between 1 and 2)
  );

CREATE OR REPLACE PROCEDURE
    csv_import(filepath text, delimiter char default ',')
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE ('COPY peers(nickname, birthday) FROM ''' || filepath || 'peers.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY tasks(title, parent_task, max_XP) FROM ''' || filepath || 'tasks.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY checks(id, peer_nickname, task_name, check_date) FROM ''' || filepath || 'checks.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY P2P(id, check_id, peer_nickname, state, time) FROM ''' || filepath || 'p2p.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY verter(id, check_id, state, time) FROM ''' || filepath || 'verter.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY transferred_points(id, checking_nickname, checked_nickname, transferred_points) FROM ''' || filepath || 'transferred_points.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY friends(id, friend1, friend2) FROM ''' || filepath || 'friends.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY recommendations(id, nickname, recommend_nickname) FROM ''' || filepath || 'recommendations.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY XP(id, check_id, xp_received) FROM ''' || filepath || 'xp.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY time_tracking(id, nickname, date, time, state) FROM ''' || filepath || 'time_tracking.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
END
$$
;

CREATE OR REPLACE PROCEDURE
    csv_export(filepath text, delimiter char default ',')
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE ('COPY peers(nickname, birthday) TO ''' || filepath || 'peers.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY tasks(title, parent_task, max_XP) TO ''' || filepath || 'tasks.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY checks(id, peer_nickname, task_name, check_date) TO ''' || filepath || 'checks.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY P2P(id, check_id, peer_nickname, state, time) TO ''' || filepath || 'p2p.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY verter(id, check_id, state, time) TO ''' || filepath || 'verter.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY transferred_points(id, checking_nickname, checked_nickname, transferred_points) TO ''' || filepath || 'transferred_points.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY friends(id, friend1, friend2) TO ''' || filepath || 'friends.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY recommendations(id, nickname, recommend_nickname) TO ''' || filepath || 'recommendations.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY XP(id, check_id, xp_received) TO ''' || filepath || 'xp.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
    EXECUTE ('COPY time_tracking(id, nickname, date, time, state) TO ''' || filepath || 'time_tracking.csv' ||
            ''' WITH HEADER DELIMITER ''' || delimiter || '''CSV;');
END
$$
;

call csv_import('/mnt/c/Users/Gulnaz Vildanova/Documents/SQL2_Info21_v1.0-1/src/csv_for_import/', ',');
-- call csv_export('/home/sadeteen/s21/SQL/SQL2_Info21_v1.0-1/src/csv_for_export/', ',');

DROP FUNCTION IF EXISTS fnc_return_transferred_points() CASCADE;

CREATE OR REPLACE FUNCTION fnc_return_transferred_points()
    RETURNS TABLE
            (
                Peer1        varchar,
                Peer2        varchar,
                PointsAmount numeric
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

-- SELECT * FROM fnc_return_transferred_points();

DROP FUNCTION IF EXISTS fnc_print_total_xp() CASCADE;

CREATE OR REPLACE FUNCTION fnc_print_total_xp() RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$$
DECLARE
    TOTAL_XP_POINTS_INFO integer;
BEGIN
    TOTAL_XP_POINTS_INFO = SUM(transferred_points) FROM transferred_points;
    RAISE NOTICE 'TOTAL TRANSFERRED XP POINTS AMOUNT: (%)', TOTAL_XP_POINTS_INFO;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_transferred_points_notice
ON transferred_points CASCADE;

CREATE OR REPLACE TRIGGER trg_transferred_points_notice
    AFTER INSERT OR UPDATE
    ON transferred_points
    FOR EACH ROW
    EXECUTE FUNCTION fnc_print_total_xp();

DROP FUNCTION IF EXISTS fnc_return_peer_task_xp();

CREATE OR REPLACE FUNCTION fnc_return_peer_task_xp()
    RETURNS TABLE
        (
        Peer varchar,
        Task varchar,
        XP integer
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

SELECT * FROM fnc_return_peer_task_xp();

DROP FUNCTION IF EXISTS fnc_trudoholic_peers(given_date date) CASCADE;

CREATE OR REPLACE FUNCTION fnc_trudoholic_peers(given_date date)
    RETURNS TABLE
            (
                Peer varchar
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


DROP FUNCTION IF EXISTS fnc_peers_finished_1_and_2_but_not_3(task1 varchar, task2 varchar, task3 varchar) CASCADE;

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
    WHERE task_name IN (task1, task2)
      AND task_name NOT IN (task3);
END;
$$;

DROP FUNCTION IF EXISTS fnc_lucky_days_for_checks(success_checks_number int) CASCADE;

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

--1
DROP PROCEDURE IF EXISTS proc_drop_the_table(IN _table_name TEXT, IN _schema TEXT) CASCADE;

CREATE OR REPLACE PROCEDURE
    proc_drop_the_table(IN _table_name TEXT default 'TableName', IN _schema TEXT default 'public')
    LANGUAGE plpgsql
AS
$$
DECLARE
    row record;
BEGIN
    FOR row IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema = _schema
          AND table_name ILIKE _table_name || '%'
        LOOP
            EXECUTE 'DROP TABLE ' || quote_ident(row.table_name) || ' CASCADE ';
            RAISE INFO 'Dropped table: %', quote_ident(row.table_name);
        END LOOP;
END
$$
;

CALL proc_drop_the_table('TableName');

--2
DROP PROCEDURE IF EXISTS proc_list_of_functions(OUT functions_amount integer, data refcursor);

CREATE OR REPLACE PROCEDURE
    proc_list_of_functions(OUT functions_amount integer, data refcursor)
    LANGUAGE plpgsql
AS
$$
BEGIN
    DROP VIEW IF EXISTS tmp_view;
    CREATE VIEW tmp_view AS

    (
    WITH func_list AS
             (SELECT routine_name,
                     ordinal_position,
                     parameter_name
              FROM (SELECT *
                    FROM information_schema.routines
                    WHERE specific_schema = ('public')
                      AND routine_type = 'FUNCTION') AS funcs
                       JOIN
                   (SELECT *
                    FROM information_schema.parameters
                    WHERE specific_schema = ('public')
                      AND parameter_name IS NOT NULL
                      AND parameter_mode = 'IN') AS params
                   ON funcs.specific_name = params.specific_name
              ORDER BY 1, 2)

    SELECT 'FUNC NAME: ' || routine_name || ', PARAMS: ' || ARRAY_TO_STRING(ARRAY_AGG(parameter_name), ', ')
               AS list_of_functions
    FROM func_list
    GROUP BY routine_name
        );

    OPEN data FOR
        SELECT * FROM tmp_view;

    functions_amount = (SELECT COUNT(*) functions_amount FROM tmp_view);

    DROP VIEW IF EXISTS tmp_view;

END
$$
;

BEGIN;
CALL proc_list_of_functions(NULL, 'data');
FETCH ALL IN "data";
CLOSE data;
COMMIT;

--3
DROP PROCEDURE IF EXISTS proc_drop_the_trigger(OUT dropped_triggers_amount integer) CASCADE;

CREATE OR REPLACE PROCEDURE
    proc_drop_the_trigger(OUT dropped_triggers_amount integer)
    LANGUAGE plpgsql
AS
$$
DECLARE
    n_trigger text;
    m_table   text;
BEGIN

    DROP VIEW IF EXISTS tmp_view_trg;
    CREATE VIEW tmp_view_trg AS

    (
    WITH trg_list AS
             (SELECT DISTINCT trigger_name, event_object_table
              FROM information_schema.triggers
              WHERE trigger_schema = ('public'))

    SELECT *
    FROM trg_list);

    dropped_triggers_amount = (SELECT COUNT(*) dropped_triggers_amount FROM tmp_view_trg);

    FOR n_trigger, m_table IN
            (SELECT trigger_name, event_object_table FROM tmp_view_trg)
        LOOP
            EXECUTE 'DROP TRIGGER ' || n_trigger || ' ON ' || m_table || ';';
        END LOOP;


    DROP VIEW IF EXISTS tmp_view_trg;
END
$$
;

CALL proc_drop_the_trigger(NULL);

CREATE OR REPLACE PROCEDURE
    proc_find_text_in_procedure_source_code(IN text_string TEXT, data refcursor)
    LANGUAGE plpgsql
AS
$$
BEGIN

OPEN DATA FOR
     SELECT proname AS name
     FROM pg_proc
     WHERE proname IN
     	   (SELECT p.proname as "Name"
	   FROM pg_catalog.pg_proc p
	   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
	   WHERE pg_catalog.pg_function_is_visible(p.oid)
	   AND n.nspname <> 'pg_catalog'
	   AND n.nspname <> 'information_schema')
      AND prosrc ILIKE '%' || text_string || '%';

END
$$
;

BEGIN;
CALL proc_find_text_in_procedure_source_code('SELECT', 'data');
FETCH ALL IN "data";
CLOSE data;
COMMIT;

BEGIN;
CALL proc_find_text_in_procedure_source_code('peers.csv', 'data');
FETCH ALL IN "data";
CLOSE data;
COMMIT;
