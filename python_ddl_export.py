import os
import traceback

def export_table_ddl_pyspark(spark, oracle_url, user, password, table_name, schema_name, output_dir):
    """
    Exports the Table DDL, Constraints, and Ref Constraints using PySpark's underlying JVM JDBC connection.
    This avoids needing extra Python packages like cx_Oracle/oracledb, while maintaining session state.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    log_file = os.path.join(output_dir, f"LOG_{table_name}_{schema_name}.txt")
    table_file = os.path.join(output_dir, f"TABLE_{table_name}_{schema_name}.sql")
    const_file = os.path.join(output_dir, f"CONSTRAINT_{table_name}_{schema_name}.sql")
    ref_const_file = os.path.join(output_dir, f"REF_CONSTRAINT_{table_name}_{schema_name}.sql")

    def write_log(message):
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(message + "\n")

    def write_sql(filepath, content):
        if content:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content + "\n")

    # Access JVM and connect to JDBC directly to maintain a stateful session
    jvm = spark.sparkContext._gateway.jvm
    properties = jvm.java.util.Properties()
    properties.put("user", user)
    properties.put("password", password)

    conn = None
    stmt = None

    try:
        conn = jvm.java.sql.DriverManager.getConnection(oracle_url, properties)
        stmt = conn.createStatement()

        # Disable certain DDL components in the current session
        disable_params_sql = """
        BEGIN
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', FALSE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', FALSE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', TRUE);
        END;
        """
        stmt.execute(disable_params_sql)

        table_found = False

        # 1. Fetch TABLE DDL
        try:
            rs = stmt.executeQuery(f"SELECT DBMS_METADATA.GET_DDL('TABLE', '{table_name}', '{schema_name}') FROM DUAL")
            if rs.next():
                clob = rs.getClob(1)
                if clob:
                    table_ddl = clob.getSubString(1, clob.length())
                    write_sql(table_file, table_ddl)
                    table_found = True
            rs.close()
        except Exception as e:
            err_msg = str(e)
            if "ORA-31603" in err_msg or "ORA-31608" in err_msg:
                write_log(f"Table {schema_name}.{table_name} not found.")
            else:
                write_log(f"Unexpected error fetching TABLE DDL for {schema_name}.{table_name}: {err_msg}")

        # Only fetch constraints if the base table exists
        if table_found:
            # 2. Fetch CONSTRAINT DDL
            try:
                rs = stmt.executeQuery(f"SELECT DBMS_METADATA.GET_DEPENDENT_DDL('CONSTRAINT', '{table_name}', '{schema_name}') FROM DUAL")
                if rs.next():
                    clob = rs.getClob(1)
                    if clob:
                        const_ddl = clob.getSubString(1, clob.length())
                        write_sql(const_file, const_ddl)
                rs.close()
            except Exception as e:
                err_msg = str(e)
                if "ORA-31608" in err_msg:
                    write_log(f"No basic constraints found for {schema_name}.{table_name}.")
                else:
                    write_log(f"Unexpected error fetching CONSTRAINT DDL for {schema_name}.{table_name}: {err_msg}")

            # 3. Fetch REF_CONSTRAINT DDL
            try:
                rs = stmt.executeQuery(f"SELECT DBMS_METADATA.GET_DEPENDENT_DDL('REF_CONSTRAINT', '{table_name}', '{schema_name}') FROM DUAL")
                if rs.next():
                    clob = rs.getClob(1)
                    if clob:
                        ref_ddl = clob.getSubString(1, clob.length())
                        write_sql(ref_const_file, ref_ddl)
                rs.close()
            except Exception as e:
                err_msg = str(e)
                if "ORA-31608" in err_msg:
                    write_log(f"No referential constraints found for {schema_name}.{table_name}.")
                else:
                    write_log(f"Unexpected error fetching REF_CONSTRAINT DDL for {schema_name}.{table_name}: {err_msg}")

        # Reset parameters
        enable_params_sql = """
        BEGIN
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', TRUE);
        END;
        """
        stmt.execute(enable_params_sql)

    except Exception as e:
        write_log(f"Critical error during execution: {traceback.format_exc()}")

        # Ensure parameters are reset even if an error occurs mid-way
        try:
            if stmt:
                stmt.execute("""
                BEGIN
                    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', TRUE);
                    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', TRUE);
                    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', TRUE);
                    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', TRUE);
                END;
                """)
        except Exception:
            pass

    finally:
        if stmt:
            stmt.close()
        if conn:
            conn.close()
