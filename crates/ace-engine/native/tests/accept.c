/* Exercise the real shim with deterministic SDK accept outcomes. */
#define srt_accept fixture_accept
#define srt_getlasterror fixture_error
#define srt_getsockstate fixture_state
#include "../srt_ingest.c"
#include <assert.h>
static int error_code;
static SRT_SOCKSTATUS listener_state;
SRTSOCKET fixture_accept(SRTSOCKET socket, struct sockaddr *address, int *length) {
    (void)socket; (void)address; (void)length;
    return SRT_INVALID_SOCK;
}
int fixture_error(int *system_error) {
    (void)system_error;
    return error_code;
}
SRT_SOCKSTATUS fixture_state(SRTSOCKET socket) {
    (void)socket;
    return listener_state;
}
int main(void) {
    int accepted = -1;
    listener_state = SRTS_LISTENING;
    error_code = SRT_ESCLOSED;
    assert(op_srt_accept(1, &accepted) == 2); /* disappeared queued peer */
    assert(accepted == -1); /* no handle escaped SDK ownership */
    listener_state = SRTS_CLOSED;
    assert(op_srt_accept(1, &accepted) == -1);
    error_code = SRT_EASYNCRCV;
    assert(op_srt_accept(1, &accepted) == -1); /* closed listener never retry */
    listener_state = SRTS_LISTENING;
    assert(op_srt_accept(1, &accepted) == 0);
    error_code = SRT_EINVSOCK;
    assert(op_srt_accept(1, &accepted) == -1);
    return 0;
}
