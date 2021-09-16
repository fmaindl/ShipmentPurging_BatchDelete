create or replace 
PROCEDURE                     shipment_refID_purge
IS
  Proc_start_time NUMBER := DBMS_UTILITY.get_time;
  curr_time       VARCHAR2(16) := substr(to_char(systimestamp), 1, 15);
  del_count       NUMBER := 0;
  total_count     NUMBER := 0;
  iteration_id NUMBER;
  cutoff_days INTEGER;
  CURSOR del_record_cursor
  IS
    (SELECT rowid
    FROM SCHEMA.rfrc_num_t rfrc
    WHERE rfrc.comp_elmt_id   IS NOT NULL
    AND rfrc.rfrc_num_typ     <> '1003'
    AND rfrc.rfrc_num_qlfr_id <> '1142'
    AND rfrc.CRTD_DTT          <
      (SELECT CURRENT_DATE - cutoff_days FROM dual
      )
    );
  BEGIN
    DBMS_OUTPUT.put_line('Start time: ' || curr_time );
    SELECT iter_log_id, cutoff_parameter INTO iteration_id, cutoff_days FROM SCHEMA.delshpmdetl_proc_logger ORDER BY iter_log_id DESC FETCH FIRST 1 ROW WITH ties;
    FOR rec IN del_record_cursor
    LOOP
      DELETE FROM SCHEMA.rfrc_num_t rfrc WHERE rowid = rec.rowid;
      del_count     := del_count   + 1;
      total_count   := total_count + 1;
      IF (del_count >= 1000) THEN
        COMMIT;
        del_count := 0;
      END IF;
    END LOOP;
    curr_time := substr(to_char(systimestamp), 1, 15);
    DBMS_OUTPUT.put_line ('Iteration ID ' || iteration_id || ' completed for data older than ' || cutoff_days || ' days');
    DBMS_OUTPUT.put_line ('' || total_count || ' entries deleted in ' || round(((DBMS_UTILITY.get_time - Proc_start_time)/6000),2) || ' minutes');
    DBMS_OUTPUT.put_line('End time: ' || curr_time );
    UPDATE SCHEMA.delshpmdetl_proc_logger SET completion_date = (SELECT sysdate FROM dual), rows_deleted = total_count WHERE iter_log_id = iteration_id;
    COMMIT;
    INSERT INTO SCHEMA.delshpmdetl_proc_logger delshpmdetl_proc_logger (iter_log_id,cutoff_parameter,created_date) VALUES ((iteration_id + 1),(case when cutoff_days <= 35 then 14 else cutoff_days-25 end),(select sysdate from dual));
    COMMIT;
    DBMS_OUTPUT.put_line('Logging activity completed. Information can be retrieved in the SCHEMA.delshpmdetl_proc_logger table.');
  EXCEPTION
  WHEN NOT_LOGGED_ON THEN
    DBMS_OUTPUT.put_line ('Table purging unsuccessful. Your program issues a database call without being connected to Oracle.');
  WHEN PROGRAM_ERROR THEN
    DBMS_OUTPUT.put_line ('Table purging unsuccessful. PL/SQL has an internal problem.');
  WHEN STORAGE_ERROR THEN
    DBMS_OUTPUT.put_line ('Table purging unsuccessful. PL/SQL runs out of memory or memory has been corrupted.');
  WHEN TIMEOUT_ON_RESOURCE THEN
    DBMS_OUTPUT.put_line ('A time-out occurs while Oracle is waiting for a resource.');
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE ('Table purging unsuccessful. Some unforseen error occured');
  END;