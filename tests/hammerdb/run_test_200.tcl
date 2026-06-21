dbset db pg
dbset bm tproc-c

diset connection pg_host "192.168.15.3"
diset connection pg_port "7432"

diset tpcc pg_user "keel_usr_cli"
diset tpcc pg_pass "qaz123"
diset tpcc pg_dbase "keel_db"

diset tpcc pg_superuser "keel_usr_cli"
diset tpcc pg_superuserpass "qaz123"

diset tpcc pg_driver timed
diset tpcc pg_duration 2
diset tpcc pg_rampup 1
diset tpcc pg_timeprofile false
diset tpcc pg_raiseerror true

vuset logtot stderr
vuset vu 50

vucreate
vurun
