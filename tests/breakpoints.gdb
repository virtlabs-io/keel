# breakpoints.gdb
# cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
# cmake --build build-debug --target keel -j$(nproc)
# gdb -x tests/breakpoints.gdb --args ./build-debug/src/main/keel -c ./etc/keel-pg-test-debug.ini
# alternativelly:
# gdb -x tests/breakpoints.gdb --args ./build-debug/src/main/keel -c ./etc/keel-pg-test-debug.ini -w 1
break worker.c:659
break worker.c:4125
break worker.c:4286
break backend_pool.c:757
break backend_connect_async.c:248
break worker.c:1623
break worker.c:1424
break worker.c:2810
break engine_flow.c:1632
break postgres_flow.c:2473
break backend_pool.c:1173
break engine_flow.c:1312
break worker.c:3788
break engine_flow.c:4502
break postgres_flow.c:3920
break backend_pool.c:1732
