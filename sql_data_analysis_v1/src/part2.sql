CREATE OR REPLACE PROCEDURE
    p2p_add(checked_nickname varchar, checker_nickname varchar, task_name varchar, p2p_check_status check_status,
            check_time time)
    LANGUAGE plpgsql
AS
$$
BEGIN
    IF p2p_check_status = 'start' THEN
        INSERT INTO checks(id, peer_nickname, task_name, check_date)
        VALUES ((SELECT MAX(id) + 1 FROM checks), checked_nickname, task_name, CURRENT_DATE);
        INSERT INTO p2p(id, check_id, peer_nickname, state, time)
        VALUES ((SELECT MAX(id) + 1 FROM p2p),
                (SELECT MAX(id) FROM checks),
                checker_nickname,
                p2p_check_status,
                check_time);
    ELSE
        INSERT INTO p2p(id, check_id, peer_nickname, state, time)
        VALUES ((SELECT MAX(id) + 1 FROM p2p),
                (SELECT check_id
                    FROM (
                    SELECT *, COUNT(*) OVER (PARTITION BY check_id) AS cnt
                    FROM p2p ) AS p
                    WHERE p.cnt = 1),
                checker_nickname,
                p2p_check_status,
                check_time);
    END IF;
END
$$
;

--call p2p_add('meplese', 'quicials', 'C4_s21_math', 'start', '11:12:13');
--call p2p_add('meplese', 'quicials', 'C4_s21_math', 'success', '11:22:13');

CREATE OR REPLACE PROCEDURE
    verter_add(checked_nickname varchar, task varchar, verter_check_status check_status,
            check_time time)
    LANGUAGE plpgsql
AS
$$
BEGIN
    INSERT INTO verter(id, check_id, state, time)
    VALUES ((SELECT MAX(id) + 1 FROM verter),
            (SELECT checks.id
             FROM checks
                      JOIN p2p on checks.id = p2p.check_id
             WHERE checks.peer_nickname = checked_nickname
               AND checks.task_name = task
               AND p2p.state = 'success'
             ORDER BY checks.check_date DESC
             LIMIT 1),
            verter_check_status,
            check_time);
END
$$
;

--call verter_add('meplese', 'C4_s21_math', 'start', '11:22:13');
--call verter_add('meplese', 'C4_s21_math', 'failure', '11:23:13');

CREATE OR REPLACE FUNCTION fnc_insert_start_review_on_p2p() RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$$
DECLARE
    CHECKING_PEER varchar := NEW.peer_nickname;
    CHECKED_PEER  varchar := (SELECT peer_nickname
                              FROM checks
                              WHERE id = NEW.check_id);
BEGIN
    IF (SELECT exists ((SELECT id FROM transferred_points WHERE checking_nickname = CHECKING_PEER)
                       INTERSECT
                       (SELECT id FROM transferred_points WHERE checked_nickname = CHECKED_PEER))) THEN
        UPDATE transferred_points
        SET transferred_points = transferred_points + 1
        WHERE id = ((SELECT id FROM transferred_points WHERE checking_nickname = CHECKING_PEER)
               INTERSECT
               (SELECT id FROM transferred_points WHERE checked_nickname = CHECKED_PEER));
    ELSE
        INSERT INTO transferred_points(id, checking_nickname, checked_nickname, transferred_points)
        VALUES ((SELECT MAX(id) + 1 FROM transferred_points),
                CHECKING_PEER,
                CHECKED_PEER,
                1);
    END IF;
    RETURN NULL;
END
$$
;

CREATE OR REPLACE TRIGGER trg_insert_start_review_on_p2p
    AFTER INSERT
    ON p2p
    FOR EACH ROW
    WHEN (NEW.state = 'start')
EXECUTE FUNCTION fnc_insert_start_review_on_p2p();

--call p2p_add('quicials', 'meplese', 'C4_s21_math', 'start', '11:12:13');

CREATE OR REPLACE FUNCTION fnc_check_insert_xp() RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$$
DECLARE
    CHECK_EXIST_IN_P2P  bool    := (SELECT exists (SELECT 1 FROM p2p WHERE check_id = NEW.check_id LIMIT 1));
    XP_LIMIT            integer := (SELECT max_xp
                                    FROM tasks
                                             JOIN checks ON tasks.title = checks.task_name
                                    WHERE checks.id = NEW.check_id);
    P2P_CHECK_STATUS    varchar := (SELECT state
                                    FROM p2p
                                             JOIN checks ON p2p.check_id = checks.id
                                    WHERE checks.id = NEW.check_id
                                    ORDER BY state DESC
                                    LIMIT 1);
    VERTER_CHECK_STATUS varchar := (SELECT state
                                    FROM verter
                                             JOIN checks ON verter.check_id = checks.id
                                    WHERE checks.id = NEW.check_id
                                    ORDER BY state DESC
                                    LIMIT 1);
BEGIN
    IF CHECK_EXIST_IN_P2P = false THEN
        RAISE EXCEPTION 'Запись еще не приступала к p2p проверке';
    ELSEIF NEW.xp_received > XP_LIMIT THEN
        RAISE EXCEPTION 'Количество XP превышает максимальное доступное для проверяемой задачи (%)', XP_LIMIT;
    ELSEIF P2P_CHECK_STATUS = 'start' THEN
        RAISE EXCEPTION 'Запись еще не прошла p2p проверку';
    ELSEIF P2P_CHECK_STATUS = 'failure' THEN
        RAISE EXCEPTION 'p2p проверка имеет статус failure';
    ELSEIF VERTER_CHECK_STATUS = 'start' THEN
        RAISE EXCEPTION 'Запись еще не прошла проверку у Вертера';
    ELSEIF VERTER_CHECK_STATUS = 'failure' THEN
        RAISE EXCEPTION 'Проверка у Вертера имеет статус failure';
    ELSE
        RETURN NEW;
    END IF;
END
$$
;

CREATE OR REPLACE TRIGGER trg_check_insert_xp
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION fnc_check_insert_xp();

--Запись еще не приступала к p2p проверке
--insert into xp(id, check_id, xp_received) VALUES (8,30,200);

--Количество XP превышает максимальное доступное для проверяемой задачи (%)
-- insert into xp(id, check_id, xp_received) VALUES (8,9,2000);

--Запись еще не прошла p2p проверку
-- insert into xp(id, check_id, xp_received) VALUES (8,19,300);

--p2p проверка имеет статус failure
-- insert into xp(id, check_id, xp_received) VALUES (8,16,200);

--Проверка у Вертера имеет статус failure
-- insert into xp(id, check_id, xp_received) VALUES (8,18,200);

--Успешная вставка
-- DELETE FROM p2p WHERE id >=35;
-- call p2p_add('quicials', 'meplese', 'C4_s21_math', 'start', '11:12:13');
-- call p2p_add('quicials', 'meplese', 'C4_s21_math', 'success', '11:22:13');
-- call verter_add('quicials', 'C4_s21_math', 'start', '11:22:13');
-- call verter_add('quicials', 'C4_s21_math', 'success', '11:23:13');
-- insert into xp(id, check_id, xp_received) VALUES (13,20,300);
