#!/usr/bin/env bash

USER=$(id -un ${UID})

mkdir -p ${ROOT_DIR_PREFIX}

chown -R ${USER} /app
chown -R ${USER} ${ROOT_DIR_PREFIX}

su ${USER} -c \
"/app/bin/vaxine foreground \
    -riak_core handoff_port ${HANDOFF_PORT} \
    -riak_core ring_creation_size ${RING_SIZE} \
    -antidote txn_cert ${ANTIDOTE_TXN_CERT} \
    -antidote txn_prot ${ANTIDOTE_TXN_PROT} \
    -antidote recover_from_log ${ANTIDOTE_RECOVER_FROM_LOG} \
    -antidote recover_metadata_on_start ${ANTIDOTE_RECOVER_METADATA_ON_START} \
    -antidote sync_log ${ANTIDOTE_SYNC_LOG} \
    -antidote enable_logging ${ANTIDOTE_ENABLE_LOGGING} \
    -antidote auto_start_read_servers ${ANTIDOTE_AUTO_START_READ_SERVERS} \
    -antidote logreader_port ${LOGREADER_PORT} \
    -antidote pubsub_port ${PBSUB_PORT} \
    -ranch pb_port ${ANTIDOTE_PB_PORT} \
    -vx_server pb_port ${VAXINE_PB_PORT} \
    -antidote_stats metrics_port ${METRICS_PORT} \
    -kernel logger_level ${DEBUG_LOGGER_LEVEL} \
    -kernel inet_dist_listen_min ${ERLANG_DIST_PORT_MIN} \
    -kernel inet_dist_listen_max ${ERLANG_DIST_PORT_MAX}"
