import isql

db_connection = isql.db_open("test_configuration")

result = isql.q(db_connection, """
        SELECT %s AS test_field 
        """,
            ("test_content",)
        )

for r in result:
    print(r["test_field"])

isql.close(db_connection)

