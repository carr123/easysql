# easysql
this's a golang library for mysql/postgresql/cockroachdb.
it's easy to switch backend dbserver between mysql, postgresql and cockroachdb with little change in your golang code.

features:
1. you should write SQL clause to talk to backend db server.
2. data type binding between database and golang type. string, int, float, date, datetime, string[], int[], jsonb, etc.
3. support transfer to default value if one db column is NULL
