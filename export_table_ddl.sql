CREATE OR REPLACE PROCEDURE export_table_ddl (
    p_table_name  IN VARCHAR2,
    p_schema_name IN VARCHAR2,
    p_os_path     IN VARCHAR2
) IS
    v_dir_name          VARCHAR2(30) := 'DDL_EXPORT_DIR_TMP';
    v_actual_dir_name   VARCHAR2(30);

    v_table_ddl         CLOB;
    v_constraint_ddl    CLOB;
    v_ref_constraint_ddl CLOB;

    v_log_filename      VARCHAR2(200);
    v_table_filename    VARCHAR2(200);
    v_const_filename    VARCHAR2(200);
    v_ref_const_filename VARCHAR2(200);

    v_table_found       BOOLEAN := FALSE;

    e_object_not_found  EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_object_not_found, -31608);

    e_table_not_found   EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_table_not_found, -31603);

    -- Helper procedure to write CLOB to file
    PROCEDURE write_clob_to_file(p_dir IN VARCHAR2, p_filename IN VARCHAR2, p_clob IN CLOB) IS
        v_out_file UTL_FILE.FILE_TYPE;
        v_offset   NUMBER := 1;
        v_amount   NUMBER := 32767;
        v_len      NUMBER;
        v_buffer   VARCHAR2(32767);
    BEGIN
        IF p_clob IS NULL THEN
            RETURN;
        END IF;

        v_len := DBMS_LOB.GETLENGTH(p_clob);
        v_out_file := UTL_FILE.FOPEN(p_dir, p_filename, 'w', 32767);

        WHILE v_offset <= v_len LOOP
            v_amount := LEAST(32767, v_len - v_offset + 1);
            DBMS_LOB.READ(p_clob, v_amount, v_offset, v_buffer);
            UTL_FILE.PUT(v_out_file, v_buffer);
            UTL_FILE.FFLUSH(v_out_file);
            v_offset := v_offset + v_amount;
        END LOOP;

        UTL_FILE.NEW_LINE(v_out_file);
        UTL_FILE.FCLOSE(v_out_file);
    EXCEPTION
        WHEN OTHERS THEN
            IF UTL_FILE.IS_OPEN(v_out_file) THEN
                UTL_FILE.FCLOSE(v_out_file);
            END IF;
            RAISE;
    END write_clob_to_file;

    -- Helper procedure to append to log file
    PROCEDURE write_to_log(p_dir IN VARCHAR2, p_filename IN VARCHAR2, p_message IN VARCHAR2) IS
        v_out_file UTL_FILE.FILE_TYPE;
    BEGIN
        -- 'a' for append mode
        v_out_file := UTL_FILE.FOPEN(p_dir, p_filename, 'a');
        UTL_FILE.PUT_LINE(v_out_file, p_message);
        UTL_FILE.FCLOSE(v_out_file);
    EXCEPTION
        WHEN OTHERS THEN
            IF UTL_FILE.IS_OPEN(v_out_file) THEN
                UTL_FILE.FCLOSE(v_out_file);
            END IF;
            RAISE;
    END write_to_log;

BEGIN
    -- Determine or create the directory object
    BEGIN
        SELECT directory_name
        INTO v_actual_dir_name
        FROM all_directories
        WHERE directory_path = p_os_path
        AND ROWNUM = 1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_actual_dir_name := v_dir_name;
            EXECUTE IMMEDIATE 'CREATE OR REPLACE DIRECTORY ' || v_actual_dir_name || ' AS ''' || p_os_path || '''';
    END;

    -- Define filenames
    v_log_filename       := 'LOG_' || p_table_name || '_' || p_schema_name || '.txt';
    v_table_filename     := 'TABLE_' || p_table_name || '_' || p_schema_name || '.sql';
    v_const_filename     := 'CONSTRAINT_' || p_table_name || '_' || p_schema_name || '.sql';
    v_ref_const_filename := 'REF_CONSTRAINT_' || p_table_name || '_' || p_schema_name || '.sql';

    -- Set initial metadata transform parameters (Disable)
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', TRUE);

    -- 1. Extract and write TABLE DDL
    BEGIN
        v_table_ddl := DBMS_METADATA.GET_DDL('TABLE', p_table_name, p_schema_name);
        write_clob_to_file(v_actual_dir_name, v_table_filename, v_table_ddl);
        v_table_found := TRUE;
    EXCEPTION
        WHEN e_table_not_found OR e_object_not_found THEN
            write_to_log(v_actual_dir_name, v_log_filename, 'Table ' || p_schema_name || '.' || p_table_name || ' not found.');
            v_table_found := FALSE;
    END;

    -- Only attempt to extract constraints if the table itself exists
    IF v_table_found THEN
        -- 2. Extract and write CONSTRAINT DDL
        BEGIN
            v_constraint_ddl := DBMS_METADATA.GET_DEPENDENT_DDL('CONSTRAINT', p_table_name, p_schema_name);
            write_clob_to_file(v_actual_dir_name, v_const_filename, v_constraint_ddl);
        EXCEPTION
            WHEN e_object_not_found THEN
                write_to_log(v_actual_dir_name, v_log_filename, 'No basic constraints found for ' || p_schema_name || '.' || p_table_name || '.');
        END;

        -- 3. Extract and write REF_CONSTRAINT DDL
        BEGIN
            v_ref_constraint_ddl := DBMS_METADATA.GET_DEPENDENT_DDL('REF_CONSTRAINT', p_table_name, p_schema_name);
            write_clob_to_file(v_actual_dir_name, v_ref_const_filename, v_ref_constraint_ddl);
        EXCEPTION
            WHEN e_object_not_found THEN
                write_to_log(v_actual_dir_name, v_log_filename, 'No referential constraints found for ' || p_schema_name || '.' || p_table_name || '.');
        END;
    END IF;

    -- Reset metadata transform parameters (Enable)
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', TRUE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', TRUE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', TRUE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', TRUE);

EXCEPTION
    WHEN OTHERS THEN
        -- Ensure parameters are reset even if an unexpected error occurs
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', TRUE);

        -- Log the unexpected error so we don't fail silently
        BEGIN
            write_to_log(v_actual_dir_name, v_log_filename, 'Unexpected error occurred: ' || SQLERRM);
        EXCEPTION
            WHEN OTHERS THEN
                NULL; -- If logging fails, just continue and let the error raise
        END;
        -- We do not RAISE here since the user requested:
        -- "if the table or any other object are presnt write it to log file instead of failing the procedure"
END export_table_ddl;
/
