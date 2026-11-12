/* Header-checked private boundary to the system reference SRT library. */
#include <srt.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#if SRT_VERSION_VALUE < 0x010507
#error "Outpace requires libsrt >= 1.5.7"
#endif

struct op_policy {
    int32_t receive_bytes, send_bytes, flow_packets, key_bytes;
    int32_t nonblocking_read, nonblocking_write;
};
uint32_t op_srt_version(void) { return srt_getversion(); }
/* Never let native peer-controlled text bypass the daemon's fixed-code logs. */
static void op_quiet_log(void *opaque, int level, const char *file, int line,
                         const char *area, const char *message) {
    (void)opaque; (void)level; (void)file; (void)line; (void)area; (void)message;
}
int op_srt_start(void) {
    srt_setloghandler(NULL, op_quiet_log);
    return srt_startup();
}
int op_srt_cleanup(void) { return srt_cleanup(); }
int op_srt_close(int socket) { return srt_close(socket); }

int op_srt_receiver(int latency, const unsigned char *key, int key_len) {
    if (latency < 20 || latency > 2000 || !key || key_len < 10 || key_len > 79 || memchr(key, 0, (size_t)key_len)) return -1;
    SRTSOCKET socket = srt_create_socket();
    if (socket == SRT_INVALID_SOCK) return -1;
    int live = SRTT_LIVE, off = 0, on = 1, flow = 1024, bytes = 1048576, idle = 5000;
    struct linger linger = {0, 0};
#define SET(option, value) do { if (srt_setsockflag(socket, option, &(value), sizeof(value)) != 0) goto fail; } while (0)
    SET(SRTO_TRANSTYPE, live);
    SET(SRTO_FC, flow);
    SET(SRTO_RCVBUF, bytes);
    SET(SRTO_SNDBUF, bytes);
    SET(SRTO_RCVSYN, off);
    SET(SRTO_SNDSYN, off);
    SET(SRTO_ENFORCEDENCRYPTION, on);
    SET(SRTO_RCVLATENCY, latency);
    SET(SRTO_PEERIDLETIMEO, idle);
    SET(SRTO_LINGER, linger);
    /* PBKEYLEN intentionally stays zero: this is a unidirectional receiver. */
    if (srt_setsockflag(socket, SRTO_PASSPHRASE, key, key_len) != 0) goto fail;
    return socket;
fail:
    srt_close(socket);
    return -1;
#undef SET
}
int op_srt_policy(int socket, struct op_policy *policy) {
    int length;
    bool value;
    if (!policy) return -1;
#define GET(option, field) do { length = sizeof(policy->field); if (srt_getsockflag(socket, option, &policy->field, &length) != 0 || length != sizeof(policy->field)) return -1; } while (0)
    GET(SRTO_RCVBUF, receive_bytes);
    GET(SRTO_SNDBUF, send_bytes);
    GET(SRTO_FC, flow_packets);
    GET(SRTO_PBKEYLEN, key_bytes);
    length = sizeof(value);
    if (srt_getsockflag(socket, SRTO_RCVSYN, &value, &length) != 0) return -1;
    policy->nonblocking_read = !value;
    length = sizeof(value);
    if (srt_getsockflag(socket, SRTO_SNDSYN, &value, &length) != 0) return -1;
    policy->nonblocking_write = !value;
    return 0;
#undef GET
}

#include <arpa/inet.h>
int op_srt_listen(int socket, const char *ip, uint16_t port, uint32_t scope,
                  uint32_t flow, int backlog, uint16_t *bound_port) {
    struct sockaddr_storage storage;
    int length;
    memset(&storage, 0, sizeof(storage));
    if (!ip || !bound_port || backlog < 1 || backlog > 16) return -1;
    if (strchr(ip, ':')) {
        struct sockaddr_in6 *address = (struct sockaddr_in6 *)&storage;
        address->sin6_family = AF_INET6;
        address->sin6_port = htons(port);
        address->sin6_scope_id = scope;
        address->sin6_flowinfo = htonl(flow);
        if (inet_pton(AF_INET6, ip, &address->sin6_addr) != 1) return -1;
        length = sizeof(*address);
    } else {
        struct sockaddr_in *address = (struct sockaddr_in *)&storage;
        address->sin_family = AF_INET;
        address->sin_port = htons(port);
        if (inet_pton(AF_INET, ip, &address->sin_addr) != 1) return -1;
        length = sizeof(*address);
    }
    if (srt_bind(socket, (struct sockaddr *)&storage, length) != 0 || srt_listen(socket, backlog) != 0) return -1;
    length = sizeof(storage);
    if (srt_getsockname(socket, (struct sockaddr *)&storage, &length) != 0) return -1;
    *bound_port = storage.ss_family == AF_INET6
        ? ntohs(((struct sockaddr_in6 *)&storage)->sin6_port)
        : ntohs(((struct sockaddr_in *)&storage)->sin_port);
    return 0;
}
int op_srt_accept(int listener, int *accepted) {
    if (!accepted) return -1;
    int socket = srt_accept(listener, NULL, NULL);
    if (socket == SRT_INVALID_SOCK) {
        int error = srt_getlasterror(NULL);
        /* ESCLOSED can refer to a queued peer removed before SDK lookup.
           Only retry while our independently owned listener remains live. */
        if (srt_getsockstate(listener) != SRTS_LISTENING) return -1;
        if (error == SRT_ESCLOSED) return 2;
        return error == SRT_EASYNCRCV ? 0 : -1;
    }
    *accepted = socket;
    return 1;
}
int op_srt_stream_id(int socket, unsigned char *buffer, int capacity, int *length) {
    if (!buffer || !length || capacity != 65) return -1;
    *length = capacity;
    return srt_getsockflag(socket, SRTO_STREAMID, buffer, length);
}
int op_srt_encrypted(int socket) {
    int state, length = sizeof(state);
    if (srt_getsockflag(socket, SRTO_RCVKMSTATE, &state, &length) != 0) return -1;
    return state == SRT_KM_S_SECURED ? 1 : 0;
}
int op_srt_receive(int socket, unsigned char *buffer, int capacity, int *length) {
    if (!buffer || !length || capacity < 1 || capacity > 65536) return -1;
    int result = srt_recvmsg(socket, (char *)buffer, capacity);
    if (result > 0) { *length = result; return 1; }
    if (result == 0) return 2;
    int error = srt_getlasterror(NULL);
    if (error == SRT_EASYNCRCV) return 0;
    if (error == SRT_ECONNLOST || error == SRT_ENOCONN) return 2;
    /* Includes ELARGEMSG: never expose a truncated successful message. */
    return -1;
}

/* CLOSED is still retained in the SDK's garbage-collection table. */
int op_srt_retired(int socket) {
    return srt_getsockstate(socket) == SRTS_NONEXIST;
}
