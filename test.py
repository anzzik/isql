import isql

test_conn = isql.db_open("test_configuration")

result = isql.q(test_conn, """
        SELECT something FROM test_table
        WHERE
        name LIKE %s
        """,
            ("AAA%",)
        )

for r in result:
    print(r["something"])

isql.db_close(test_conn)

