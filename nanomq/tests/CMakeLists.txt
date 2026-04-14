nanomq_test(sub_handler_test)
nanomq_test(unsub_handler_test)
nanomq_test(pub_handler_test)
nanomq_test(broker_test)
nanomq_test(webhook_test)
nanomq_test(webhook_base62_test)
nanomq_test(webhook_base64_test)
nanomq_test(http_server_test)
nanomq_test(bridge_test)
nanomq_test(rule_engine_test)
nanomq_test(hashmap_test)
if (NOT WIN32)
    nanomq_test(process_test)
endif()
nanomq_test(broker_tls_test)
nanomq_test(bridge_tls_test)
nanomq_test(bridge_rap_rh_test)
nanomq_test(bridge_aws_test)
nanomq_test(bridge_muti_bridge_test)
nanomq_test(mqtt_api_test)
nanomq_test(nmq_ws_test)
nanomq_test(properties_test)

add_executable(hashmap_internal_test hashmap_internal_test.c)
target_link_libraries(hashmap_internal_test nng)
target_include_directories(hashmap_internal_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.hashmap_internal_test COMMAND hashmap_internal_test)
set_tests_properties(nanomq.hashmap_internal_test PROPERTIES TIMEOUT 60)

add_executable(cmd_proc_test cmd_proc_test.c)
target_link_libraries(cmd_proc_test nng)
target_include_directories(cmd_proc_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.cmd_proc_test COMMAND cmd_proc_test)
set_tests_properties(nanomq.cmd_proc_test PROPERTIES TIMEOUT 60)

add_executable(acl_handler_unit_test acl_handler_unit_test.c)
target_link_libraries(acl_handler_unit_test nng)
target_include_directories(acl_handler_unit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.acl_handler_unit_test COMMAND acl_handler_unit_test)
set_tests_properties(nanomq.acl_handler_unit_test PROPERTIES TIMEOUT 60)

add_executable(webhook_post_unit_test webhook_post_unit_test.c)
target_link_libraries(webhook_post_unit_test nng)
target_include_directories(webhook_post_unit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.webhook_post_unit_test COMMAND webhook_post_unit_test)
set_tests_properties(nanomq.webhook_post_unit_test PROPERTIES TIMEOUT 60)

add_executable(conf_api_unit_test conf_api_unit_test.c)
target_link_libraries(conf_api_unit_test nng)
target_include_directories(conf_api_unit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.conf_api_unit_test COMMAND conf_api_unit_test)
set_tests_properties(nanomq.conf_api_unit_test PROPERTIES TIMEOUT 60)

if (NOT WIN32)
    add_executable(plugin_unit_test plugin_unit_test.c)
    target_link_libraries(plugin_unit_test nng)
    target_include_directories(plugin_unit_test PRIVATE
            ${PROJECT_SOURCE_DIR}/include)
    add_test(NAME nanomq.plugin_unit_test COMMAND plugin_unit_test)
    set_tests_properties(nanomq.plugin_unit_test PROPERTIES TIMEOUT 60)
endif()

add_executable(plugin_user_property_unit_test plugin_user_property_unit_test.c)
target_link_libraries(plugin_user_property_unit_test nng)
target_include_directories(plugin_user_property_unit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.plugin_user_property_unit_test COMMAND plugin_user_property_unit_test)
set_tests_properties(nanomq.plugin_user_property_unit_test PROPERTIES TIMEOUT 60)

add_executable(nanomq_unit_test nanomq_unit_test.c)
target_link_libraries(nanomq_unit_test nng)
target_include_directories(nanomq_unit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.nanomq_unit_test COMMAND nanomq_unit_test)
set_tests_properties(nanomq.nanomq_unit_test PROPERTIES TIMEOUT 60)

add_executable(webhook_inproc_unit_test webhook_inproc_unit_test.c)
target_link_libraries(webhook_inproc_unit_test nng)
target_include_directories(webhook_inproc_unit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.webhook_inproc_unit_test COMMAND webhook_inproc_unit_test)
set_tests_properties(nanomq.webhook_inproc_unit_test PROPERTIES TIMEOUT 60)

add_executable(nanomq_rule_unit_test nanomq_rule_unit_test.c)
target_link_libraries(nanomq_rule_unit_test nng)
target_include_directories(nanomq_rule_unit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.nanomq_rule_unit_test COMMAND nanomq_rule_unit_test)
set_tests_properties(nanomq.nanomq_rule_unit_test PROPERTIES TIMEOUT 60)

add_executable(web_server_unit_test web_server_unit_test.c)
target_link_libraries(web_server_unit_test nng)
target_include_directories(web_server_unit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.web_server_unit_test COMMAND web_server_unit_test)
set_tests_properties(nanomq.web_server_unit_test PROPERTIES TIMEOUT 60)

add_executable(db_cli_unit_test db_cli_unit_test.c)
target_link_libraries(db_cli_unit_test nng)
target_include_directories(db_cli_unit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.db_cli_unit_test COMMAND db_cli_unit_test)
set_tests_properties(nanomq.db_cli_unit_test PROPERTIES TIMEOUT 60)

add_executable(rest_api_uri_unit_test rest_api_uri_unit_test.c)
target_link_libraries(rest_api_uri_unit_test nng)
target_include_directories(rest_api_uri_unit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
target_compile_options(rest_api_uri_unit_test PRIVATE -ffunction-sections -fdata-sections)
target_link_options(rest_api_uri_unit_test PRIVATE -Wl,--gc-sections)
add_test(NAME nanomq.rest_api_uri_unit_test COMMAND rest_api_uri_unit_test)
set_tests_properties(nanomq.rest_api_uri_unit_test PROPERTIES TIMEOUT 60)

add_executable(aws_bridge_unit_test aws_bridge_unit_test.c)
target_link_libraries(aws_bridge_unit_test nng)
target_include_directories(aws_bridge_unit_test PRIVATE
        ${PROJECT_SOURCE_DIR}/include)
add_test(NAME nanomq.aws_bridge_unit_test COMMAND aws_bridge_unit_test)
set_tests_properties(nanomq.aws_bridge_unit_test PROPERTIES TIMEOUT 60)

if(ENABLE_TAOS)
    find_library(NANOMQ_TEST_ZSTD_LIBRARY zstd)
    if(NOT NANOMQ_TEST_ZSTD_LIBRARY)
        message(FATAL_ERROR "libzstd is required for weld telemetry tests")
    endif()

    nanomq_test(broker_send_rule_test)
    nanomq_test(pub_handler_weld_rule_test)

    add_executable(taos_sink_test taos_sink_test.cpp)
    target_link_libraries(taos_sink_test nng)
    target_include_directories(taos_sink_test PRIVATE
            ${PROJECT_SOURCE_DIR}/include)
    add_test(NAME nanomq.taos_sink_test COMMAND taos_sink_test)
    set_tests_properties(nanomq.taos_sink_test PROPERTIES TIMEOUT 60)

    add_executable(weld_telemetry_test weld_telemetry_test.cpp)
    target_link_libraries(weld_telemetry_test nng ${NANOMQ_TEST_ZSTD_LIBRARY})
    target_include_directories(weld_telemetry_test PRIVATE
            ${PROJECT_SOURCE_DIR}/include)
    add_test(NAME nanomq.weld_telemetry_test COMMAND weld_telemetry_test)
    set_tests_properties(nanomq.weld_telemetry_test PROPERTIES TIMEOUT 60)

    add_executable(weld_telemetry_async_test weld_telemetry_async_test.cpp)
    target_link_libraries(weld_telemetry_async_test nng)
    target_include_directories(weld_telemetry_async_test PRIVATE
            ${PROJECT_SOURCE_DIR}/include)
    add_test(NAME nanomq.weld_telemetry_async_test COMMAND weld_telemetry_async_test)
    set_tests_properties(nanomq.weld_telemetry_async_test PROPERTIES TIMEOUT 60)

    add_executable(weld_taos_sink_test weld_taos_sink_test.cpp)
    target_link_libraries(weld_taos_sink_test nng)
    target_include_directories(weld_taos_sink_test PRIVATE
            ${PROJECT_SOURCE_DIR}/include)
    add_test(NAME nanomq.weld_taos_sink_test COMMAND weld_taos_sink_test)
    set_tests_properties(nanomq.weld_taos_sink_test PROPERTIES TIMEOUT 60)
endif()
if(ENABLE_PARQUET)
    nanomq_test(parquet_test)
endif()
if(NNG_ENABLE_QUIC)
    nanomq_test(quic_smoke_test)
endif()
