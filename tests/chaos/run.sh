#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# --- Default Configuration Variables (Overridable via Environment) ---
DB_HOST="${DB_HOST:-192.168.15.3}"
DB_PORT="${DB_PORT:-7432}"
DB_USER="${DB_USER:-keel_usr_cli}"
DB_PASS="${DB_PASS:-qaz123}"
DB_NAME="${DB_NAME:-keel_db}"

# --- Function 1: Build Schema ---
build_schema() {
    local warehouses=${1:-20}
    local build_vus=${2:-10}
    
    echo "=================================================="
    echo " Configuration:"
    echo "   Target:   ${DB_HOST}:${DB_PORT} (${DB_NAME})"
    echo "   User:     ${DB_USER}"
    echo "=================================================="
    echo " Building Schema: ${warehouses} Warehouses using ${build_vus} VUs"
    echo "=================================================="

    # Generate the Tcl script on the fly using environmental states
    cat << EOF > ./build_schema.tcl
dbset db pg
dbset bm tproc-c
diset connection pg_host "${DB_HOST}"
diset connection pg_port "${DB_PORT}"
diset tpcc pg_user "${DB_USER}"
diset tpcc pg_pass "${DB_PASS}"
diset tpcc pg_dbase "${DB_NAME}"
diset tpcc pg_defaultdbase "${DB_NAME}"
diset tpcc pg_superuser "${DB_USER}"
diset tpcc pg_superuserpass "${DB_PASS}"
diset tpcc pg_num_vu ${build_vus}
diset tpcc pg_count_ware ${warehouses}
buildschema
EOF

    # Run the container
    docker run --rm \
      -v "$(pwd)":/test \
      tpcorg/hammerdb:postgres \
      ./hammerdbcli auto /test/build_schema.tcl

    # Clean up the generated script
    rm -f ./build_schema.tcl
}

# --- Function 2: Run Load Test ---
run_test() {
    local test_vus=${1:-10}
    local duration=${2:-5}
    local rampup=${3:-2}
    
    echo "=================================================="
    echo " Configuration:"
    echo "   Target:   ${DB_HOST}:${DB_PORT} (${DB_NAME})"
    echo "   User:     ${DB_USER}"
    echo "=================================================="
    echo " Running Load Test: ${test_vus} VUs for ${duration} min (Rampup: ${rampup} min)"
    echo "=================================================="

    # Generate the Tcl script on the fly using environmental states
    cat << EOF > ./run_test.tcl
dbset db pg
dbset bm tproc-c
diset connection pg_host "${DB_HOST}"
diset connection pg_port "${DB_PORT}"
diset tpcc pg_user "${DB_USER}"
diset tpcc pg_pass "${DB_PASS}"
diset tpcc pg_dbase "${DB_NAME}"
diset tpcc pg_driver timed
diset tpcc pg_duration ${duration}
diset tpcc pg_rampup ${rampup}
diset tpcc pg_raiseerror true
vuset logot stderr
vuset vu ${test_vus}
vucreate
vurun
EOF

    # Run the container
    docker run --rm \
      -v "$(pwd)":/test \
      tpcorg/hammerdb:postgres \
      ./hammerdbcli auto /test/run_test.tcl

    # Clean up the generated script
    rm -f ./run_test.tcl
}

# --- Execution Controller / Argument Parsing ---
case "$1" in
    build)
        build_schema "$2" "$3"
        ;;
    run)
        run_test "$2" "$3" "$4"
        ;;
    *)
        echo "Usage: $0 {build|run} [parameters]"
        echo ""
        echo "Default Environment (Change by exporting variables):"
        echo "  DB_HOST=$DB_HOST, DB_PORT=$DB_PORT, DB_USER=$DB_USER, DB_NAME=$DB_NAME"
        echo ""
        echo "Examples:"
        echo "  $0 build 50 8"
        echo "  DB_HOST=10.0.0.5 DB_PORT=5432 $0 run 50 15 3"
        exit 1
        ;;
esac