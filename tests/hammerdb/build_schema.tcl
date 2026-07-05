dbset db pg
dbset bm tproc-c

# Network Details (under connection)
diset connection pg_host "192.168.15.3"
diset connection pg_port "7432"

# Credentials & Database (under tpcc)
diset tpcc pg_user "keel_usr_cli"
diset tpcc pg_pass "qaz123"
diset tpcc pg_dbase "keel_db"

# Force initial connection gate to use your target DB
diset tpcc pg_defaultdbase "keel_db"

# Administrative/Superuser Override
diset tpcc pg_superuser "keel_usr_cli"
diset tpcc pg_superuserpass "qaz123"

# Schema Options
diset tpcc pg_num_vu 10
diset tpcc pg_count_ware 20

# Build Schema
print dict
buildschema