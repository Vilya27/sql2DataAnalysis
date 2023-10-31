DROP TABLE IF EXISTS checks, friends, p2p, peers, recommendations, tasks, time_tracking, transferred_points, verter, XP cascade;
DROP TYPE IF EXISTS check_status CASCADE;
DROP PROCEDURE IF EXISTS csv_import(filepath text, delimiter char) CASCADE;
DROP PROCEDURE IF EXISTS csv_export(filepath text, delimiter char) CASCADE;

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

CALL csv_import('/mnt/c/Users/Gulnaz Vildanova/Documents/SQL2_Info21_v1.0-1/src/csv_for_import/', ',');
--CALL csv_export('/home/sadeteen/s21/SQL/SQL2_Info21_v1.0-1/src/csv_for_export/', ',');

