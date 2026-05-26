/**
 * @file test_otlp_http.c
 * @brief End-to-end test for the bounded-timeout OTLP HTTP/1.1 client.
 *
 * Spins up an in-process HTTP server thread on an ephemeral localhost
 * port that returns a configurable status code per request, then drives
 * keel_otlp_http_post through the canonical mappings:
 *   200 → OK, 400 → SERVER_REJECT, 429 → SERVER_RETRY,
 *   500 → SERVER_RETRY, no-server → CONNECT_FAILED.
 */

#include "test_utils.h"
#include "keel_otlp_http.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------------- */
/* Mock HTTP server                                                           */
/* ------------------------------------------------------------------------- */

typedef struct mock_server {
    int             listen_fd;
    uint16_t        port;
    int             status_code;
    pthread_t       thread;
    atomic_bool     stop;
    atomic_int      requests_handled;
    atomic_size_t   last_body_len;
} mock_server_t;

static void* mock_server_thread(void* arg)
{
    mock_server_t* s = arg;
    while (!atomic_load(&s->stop)) {
        struct pollfd pfd = { .fd = s->listen_fd, .events = POLLIN };
        int pr = poll(&pfd, 1, 100);
        if (pr <= 0)
            continue;
        int cfd = accept(s->listen_fd, NULL, NULL);
        if (cfd < 0)
            continue;
        char   buf[8192];
        size_t off = 0;
        size_t content_length    = 0;
        size_t headers_end_off   = 0;
        bool   have_headers      = false;
        while (off + 1 < sizeof(buf)) {
            struct pollfd cpfd = { .fd = cfd, .events = POLLIN };
            if (poll(&cpfd, 1, 1000) <= 0)
                break;
            ssize_t n = recv(cfd, buf + off, sizeof(buf) - 1 - off, 0);
            if (n <= 0)
                break;
            off += (size_t)n;
            buf[off] = '\0';
            if (!have_headers) {
                char* he = strstr(buf, "\r\n\r\n");
                if (he) {
                    have_headers    = true;
                    headers_end_off = (size_t)(he - buf) + 4;
                    char* cl = strcasestr(buf, "Content-Length:");
                    if (cl)
                        content_length = strtoul(cl + 15, NULL, 10);
                }
            }
            if (have_headers && off >= headers_end_off + content_length)
                break;
        }
        atomic_store(&s->last_body_len,
                     have_headers ? (off - headers_end_off) : 0);

        const char* reason =
            s->status_code == 200 ? "OK" :
            s->status_code == 400 ? "Bad Request" :
            s->status_code == 429 ? "Too Many Requests" :
            s->status_code == 500 ? "Internal Server Error" : "Status";
        char resp[256];
        int  rl = snprintf(resp, sizeof(resp),
            "HTTP/1.1 %d %s\r\n"
            "Content-Length: 0\r\n"
            "Connection: close\r\n"
            "\r\n",
            s->status_code, reason);
        if (rl > 0)
            (void)send(cfd, resp, (size_t)rl, MSG_NOSIGNAL);
        close(cfd);
        atomic_fetch_add(&s->requests_handled, 1);
    }
    return NULL;
}

static mock_server_t* mock_server_start(int status_code)
{
    static mock_server_t s;
    memset(&s, 0, sizeof(s));
    s.status_code = status_code;
    atomic_store(&s.stop, false);
    atomic_store(&s.requests_handled, 0);
    atomic_store(&s.last_body_len, (size_t)0);

    s.listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    TEST_ASSERT(s.listen_fd >= 0);
    int one = 1;
    setsockopt(s.listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    TEST_ASSERT(bind(s.listen_fd, (struct sockaddr*)&addr, sizeof(addr)) == 0);
    socklen_t alen = sizeof(addr);
    TEST_ASSERT(getsockname(s.listen_fd, (struct sockaddr*)&addr, &alen) == 0);
    s.port = ntohs(addr.sin_port);
    TEST_ASSERT(listen(s.listen_fd, 4) == 0);
    TEST_ASSERT(pthread_create(&s.thread, NULL, mock_server_thread, &s) == 0);
    return &s;
}

static void mock_server_stop(mock_server_t* s)
{
    atomic_store(&s->stop, true);
    pthread_join(s->thread, NULL);
    close(s->listen_fd);
}

/* ------------------------------------------------------------------------- */
/* Test cases                                                                 */
/* ------------------------------------------------------------------------- */

static keel_otlp_http_t* make_client(uint16_t port, uint32_t timeout_ms)
{
    static char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%u/v1/metrics", (unsigned)port);
    keel_otlp_http_config_t cfg = {
        .endpoint_url = url,
        .timeout_ms   = timeout_ms,
        .bearer_token = NULL,
    };
    return keel_otlp_http_create(&cfg);
}

static void post_and_expect(int status_code, keel_otlp_http_result_t expected)
{
    mock_server_t*    s = mock_server_start(status_code);
    keel_otlp_http_t* c = make_client(s->port, 2000);
    TEST_ASSERT_NOT_NULL(c);

    static const uint8_t body[] = { 0xDE, 0xAD, 0xBE, 0xEF };
    keel_otlp_http_result_t r = keel_otlp_http_post(c, body, sizeof(body));
    TEST_ASSERT_EQ((int)r, (int)expected);
    /* Server-side atomic increment may lag client return; wait briefly. */
    for (int i = 0; i < 100 && atomic_load(&s->requests_handled) < 1; ++i) {
        struct timespec ts = { 0, 10 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    TEST_ASSERT_EQ((int)atomic_load(&s->requests_handled), 1);
    TEST_ASSERT_EQ((int)atomic_load(&s->last_body_len), (int)sizeof(body));

    keel_otlp_http_destroy(c);
    mock_server_stop(s);
}

static void test_post_200(void)            { post_and_expect(200, KEEL_OTLP_HTTP_OK); }
static void test_post_400(void)            { post_and_expect(400, KEEL_OTLP_HTTP_SERVER_REJECT); }
static void test_post_429(void)            { post_and_expect(429, KEEL_OTLP_HTTP_SERVER_RETRY); }
static void test_post_500(void)            { post_and_expect(500, KEEL_OTLP_HTTP_SERVER_RETRY); }

static void test_connect_failed(void)
{
    /* Use a port we are confident nothing is listening on. Bind to one,
     * then immediately close to get a free ephemeral port. */
    int s = socket(AF_INET, SOCK_STREAM, 0);
    TEST_ASSERT(s >= 0);
    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    TEST_ASSERT(bind(s, (struct sockaddr*)&addr, sizeof(addr)) == 0);
    socklen_t alen = sizeof(addr);
    TEST_ASSERT(getsockname(s, (struct sockaddr*)&addr, &alen) == 0);
    uint16_t dead_port = ntohs(addr.sin_port);
    close(s);

    keel_otlp_http_t* c = make_client(dead_port, 500);
    TEST_ASSERT_NOT_NULL(c);
    static const uint8_t body[] = { 0x00 };
    keel_otlp_http_result_t r = keel_otlp_http_post(c, body, sizeof(body));
    /* Either CONNECT_FAILED or TIMEOUT is acceptable, depending on whether
     * the kernel returns ECONNREFUSED immediately or stalls. */
    TEST_ASSERT(r == KEEL_OTLP_HTTP_CONNECT_FAILED ||
                r == KEEL_OTLP_HTTP_TIMEOUT);
    keel_otlp_http_destroy(c);
}

static void test_bad_url(void)
{
    keel_otlp_http_config_t cfg = {
        .endpoint_url = "https://not-supported/v1/metrics",
        .timeout_ms   = 500,
    };
    TEST_ASSERT_NULL(keel_otlp_http_create(&cfg));

    keel_otlp_http_config_t cfg2 = { .endpoint_url = NULL };
    TEST_ASSERT_NULL(keel_otlp_http_create(&cfg2));
    TEST_ASSERT_NULL(keel_otlp_http_create(NULL));
}

int main(void) {
    test_bad_url();
    test_connect_failed();
    test_post_200();
    test_post_400();
    test_post_429();
    test_post_500();
    return test_summary();
}
