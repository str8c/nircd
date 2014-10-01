#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <dlfcn.h>

#define PORT 6667
#define PING_TIMEOUT 10
#define HOST "str8c.org"

typedef struct {
    int sock;
    uint16_t rlen;
    uint8_t nchannel, pinged;
    char *data;
    char name[16];
    uint8_t channel[32];
} CLIENT;

typedef struct {
    uint16_t nclient, unused[3];
    uint16_t *client;
    char name[16];
} CHANNEL;

static struct {
    uint16_t family, port;
    uint32_t ip;
    uint8_t padding[8];
} addr = {
    .family = AF_INET,
    .port = __bswap_constant_16(PORT),
};

static const struct itimerspec itimer = {
    .it_interval = {
        .tv_sec = PING_TIMEOUT,
    },
    .it_value = {
        .tv_sec = PING_TIMEOUT,
    },
};

enum {
    MSG_NICK,
    MSG_JOIN,
    MSG_PART,
    MSG_PING,
    MSG_PRIV,
    MSG_QUIT,
};

static const char *messages[] = {
    "NICK",
    "JOIN",
    "PART",
    "PING",
    "PRIVMSG",
    "QUIT",
};

static int nclient, nchannel;
static CLIENT client[65536], *fclient = client;
static CHANNEL channel[256], *fchannel = channel;

static int one = 1;

static CHANNEL* newchannel(void)
{
    CHANNEL *ch;

    ch = fchannel;
    while((++fchannel)->nclient != 0);
    nchannel++;

    return ch;
}

static CHANNEL* findchannel(char *name)
{
    CHANNEL *ch;
    int i;

    for(i = 0, ch = channel; i != nchannel; ch++) {
        if(ch->nclient) {
            if(!strcmp(name, ch->name)) {
                return ch;
            }
            i++;
        }
    }

    return NULL;
}

static _Bool addclient(CHANNEL *ch, uint16_t id)
{
    uint16_t *tmp;

    tmp = realloc(ch->client, (ch->nclient + 1) * sizeof(*ch->client));
    if(!tmp) {
        return 0;
    }

    ch->client = tmp;
    ch->client[ch->nclient++] = id;

    return 1;
}

static void removeclient(CHANNEL *ch, uint16_t id)
{
    int i;

    if(ch->nclient == 1) {
        if(ch->client[0] != id) {
            printf("snh2\n");
            return;
        }

        ch->nclient = 0;
        free(ch->client);
        nchannel--;
        return;
    }

    for(i = 0; i != ch->nclient; i++) {
        if(ch->client[i] == id) {
            ch->nclient--;
            memcpy(&ch->client[i], &ch->client[i + 1], (ch->nclient - i) * sizeof(*ch->client));
            return;
        }
    }

    printf("snh1\n");
}

static void sendpart(CLIENT *cl, CHANNEL *ch, char *buf, char *reason)
{
    int len, i;
    CLIENT *c;

    len = sprintf(buf, ":%s PART %s %s\n", cl->name, ch->name, reason);
    for(i = 0; i != ch->nclient; i++) {
        c = &client[ch->client[i]];
        send(c->sock, buf, len, 0);
    }
}

static CLIENT* findclient(char *name)
{
    CLIENT *cl;
    int i;

    for(cl = client, i = 0; i != nclient; cl++) {
        if(cl->sock) {
            if(!strcmp(cl->name, name)) {
                return cl;
            }
            i++;
        }
    }

    return NULL;
}

static CLIENT* newclient(void)
{
    CLIENT *cl;

    cl = fclient;
    while((++fclient)->sock != 0); //NOTE: should use -1 as invalid value, but using 0 because lazy
    nclient++;

    return cl;
}

static void killclient(CLIENT *cl)
{
    int i;
    uint16_t id;
    CHANNEL *ch;
    char buf[1024];

    id = (cl - client);
    close(cl->sock); cl->sock = 0;

    for(i = 0; i != cl->nchannel; i++) {
        ch = &channel[cl->channel[i]];
        removeclient(ch, id);
        sendpart(cl, ch, buf, "timeout/quit");
    }

    cl->nchannel = 0;
    cl->name[0] = 0;
    free(cl->data); cl->rlen = 0;
    nclient--;
}

#define match(word, list) _match(word, list, sizeof(list)/sizeof(*list) - 1)
static int _match(const char *word, const char **list, int i)
{
    /* i is index of last valid string in the list */
    do {
        if(!strcmp(word, list[i])) {
            return i;
        }
    } while(i--);
    return -1;
}

static char* channel_name(char *p)
{
    if(*p != '#') {
        return NULL;
    }

    while(isalnum(*(++p))); //USE CUSTOM FUNCTION
    *p = 0;

    while(p++, *p && *p != '#');

    return p;
}

static void cl_cmd(CLIENT *cl, char *cmd)
{
    int msg, len, i;
    uint16_t id;
    char *args, *a;
    CHANNEL *ch;
    CLIENT *c;
    char response[1024], *r;

    id = (cl - client);
    cl->pinged = 0;

    args = strchr(cmd, ' ');
    if(!args) {
        return;
    }
    *args++ = 0;

    //printf("%s %s\n", cmd, args);

    msg = match(cmd, messages);
    if(msg < 0) {
        return;
    }

    if(!cl->name[0] && msg != MSG_NICK) {
        return;
    }

    if(msg == MSG_PRIV) {
        a = strchr(args, ' ');
        if(!a) {
            return;
        }
        *a++ = 0;

        ch = findchannel(args);
        if(!ch) {
            return;
        }

        len = sprintf(response, ":%s PRIVMSG %s %s\n", cl->name, ch->name, a);
        for(i = 0; i != ch->nclient; i++) {
            c = &client[ch->client[i]];
            if(c == cl) {
                continue;
            }
            send(c->sock, response, len, 0);
        }
    } else if(msg == MSG_NICK) {
        if(strlen(args) >= sizeof(cl->name)) {
            args[sizeof(cl->name) - 1] = 0;
        }

        if(!args[0]) { /* empty nick */
            return;
        }

        if(!strcmp(cl->name, args)) {
            return;
        }

        if(findclient(args)) {
            len = sprintf(response, ":" HOST " 433 %s %s\n", args, args);
            send(cl->sock, response, len, 0);
            return;
        }

        if(cl->name[0]) { /* not first time setting name */
            //only send to those who need to know.. (not GLOBAL!!)
            len = sprintf(response, ":%s NICK %s\n", cl->name, args);
            for(c = client, i = 0; i != nclient; c++) {
                if(c->sock) {
                    send(c->sock, response, len, 0);
                    i++;
                }
            }
        } else {
            len = sprintf(response, ":" HOST " 372 %s :Welcome %s. There are %u users in %u channels\n:" HOST " 376 %s :New users join channel #main\n", args, args, nclient, nchannel, args);
            send(cl->sock, response, len, 0);
        }

        strcpy(cl->name, args);
    } else if(msg == MSG_JOIN) {
        while((a = channel_name(args))) {
            if(cl->nchannel == sizeof(cl->channel)) {
                return;
            }

            ch = findchannel(args);
            if(ch) {
                addclient(ch, id);
            } else {
                ch = newchannel();
                ch->nclient = 1;
                ch->client = malloc(sizeof(*ch->client));
                ch->client[0] = id;
                strncpy(ch->name, args, sizeof(ch->name) - 1);
            }

            cl->channel[cl->nchannel++] = (ch - channel);

            len = sprintf(response, ":%s JOIN %s\n", cl->name, ch->name);
            r = response + len;
            r += sprintf(r, ":" HOST " 353 %s @ %s :%s", cl->name, ch->name, cl->name);

            for(i = 0; i != ch->nclient; i++) {
                c = &client[ch->client[i]];
                if(c == cl) {
                    continue;
                }
                r += sprintf(r, " %s", c->name);

                send(c->sock, response, len, 0);
            }
            *r++ = '\n';

            len = sprintf(r, ":" HOST " 366 %s %s :End of NAMES list.\n", cl->name, ch->name);
            r += len;

            send(cl->sock, response, r - response, 0);

            args = a;
        }
    } else if(msg == MSG_PART) {
        a = strchr(args, ' ');
        if(!a) {
            return;
        }
        *a++ = 0;

        for(i = 0; i != cl->nchannel; i++) {
            ch = &channel[cl->channel[i]];
            if(!strcmp(args, ch->name)) {
                cl->nchannel--;
                memcpy(&cl->channel[i], &cl->channel[i + 1], (cl->nchannel - i) * sizeof(*cl->channel));
                removeclient(ch, id);

                sendpart(cl, ch, response, a);
                break;
            }
        }
    } else if(msg == MSG_PING) {
        len = sprintf(response, "PONG %s\n", args);
        send(cl->sock, response, len, 0);
    }
}

static int tcp_init(void)
{
    int sock, r;

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if(sock < 0) {
        return sock;
    }

    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (void*)&one, sizeof(int));

    r = bind(sock, (struct sockaddr*)&addr, sizeof(addr));
    if(r < 0) {
        printf("bind() failed\n");
        close(sock);
        return r;
    }

    r = listen(sock, SOMAXCONN);
    if(r < 0) {
        close(sock);
        return r;
    }

    return sock;
}

int main(int argc, char *argv[])
{
    int efd, tfd, sock, n, csock, len, ncl;
    socklen_t addrlen;
    uint64_t exp;
    struct epoll_event events[16], *ev, *ev_last;
    CLIENT *cl;
    char buf[256];
    char *s, *ss;

    sock = tcp_init();
    if(sock < 0) {
        return 1;
    }

    if((tfd = timerfd_create(CLOCK_MONOTONIC, 0)) < 0) {
        goto EXIT_CLOSE_SOCK;
    }

    timerfd_settime(tfd, 0, &itimer, NULL);

    if((efd = epoll_create(1)) < 0) {
        goto EXIT_CLOSE_TFD;
    }

    ev = &events[0];
    ev->events = EPOLLIN;
    ev->data.fd = -1;
    epoll_ctl(efd, EPOLL_CTL_ADD, sock, ev); //check epoll_ctl error
    ev->events = EPOLLIN;
    ev->data.fd = -2;
    epoll_ctl(efd, EPOLL_CTL_ADD, tfd, ev); //check epoll_ctl error

    addrlen = 0;

    do {
        if((n = epoll_wait(efd, events, 16, -1)) < 0) {
            break;
        }

        ev = events;
        ev_last = ev + n;
        do {
            if(ev->data.fd < 0) {
                if(ev->data.fd == -1) { /* listening socket event */
                    csock = accept(sock, (struct sockaddr*)&addr, &addrlen);
                    if(csock < 0) {
                        printf("accept failed\n");
                        continue;
                    }

                    cl = newclient();
                    cl->sock = csock;

                    ev->events = EPOLLIN;// | EPOLLET;
                    ev->data.fd = (cl - client);
                    epoll_ctl(efd, EPOLL_CTL_ADD, csock, ev); //handle epoll_ctl error
                } else { /* timer event */
                    read(tfd, &exp, 8);

                    for(n = 0, cl = client, ncl = nclient; n != ncl; cl++) {
                        if(cl->sock) {
                            if(cl->pinged) {
                                killclient(cl);
                            } else {
                                static const char ping[] = "PING :0\n";
                                send(cl->sock, ping, sizeof(ping) - 1, 0);
                                cl->pinged = 1;
                            }
                            n++;
                        }
                    }
                }
            } else {
                cl = &client[ev->data.fd];
                len = recv(cl->sock, buf, sizeof(buf) - 1, 0);
                if(len <= 0) {
                    killclient(cl);
                    continue;
                }

                buf[len] = 0; /* null terminate for string operations */
                if(strlen(buf) != len) { /* verify that there are no other null characters */
                    continue;
                }

                /* clean this shit up */
                ss = buf;
                while((s = strchr(ss, '\n'))) {
                    if(cl->rlen) {
                        *s++ = 0;
                        cl->data = realloc(cl->data, cl->rlen + (s - ss));
                        memcpy(cl->data + cl->rlen, ss, (s - ss));
                        cl->rlen += (s - ss);
                        cl->data[cl->rlen] = 0;

                        if(cl->data[cl->rlen - 1] == '\r') {
                            cl->data[cl->rlen - 1] = 0;
                        }

                        cl_cmd(cl, cl->data);

                        free(cl->data); cl->data = NULL;
                        cl->rlen = 0;
                    } else {
                        if(s != ss && *(s - 1) == '\r') {
                            *(s - 1) = 0;
                        }
                        *s++ = 0;
                        cl_cmd(cl, ss);
                    }
                    ss = s;
                }

                /* todo: add a limit, etc */
                len -= (ss - buf);
                if(len) { /* data remaining */
                    cl->data = realloc(cl->data, cl->rlen + len);
                    memcpy(cl->data + cl->rlen, ss, len);
                    cl->rlen += len;
                }
            }
        } while(ev++, ev != ev_last);
    } while(1);

    close(efd);
EXIT_CLOSE_TFD:
    close(tfd);
EXIT_CLOSE_SOCK:
    close(sock);
    return 1;
}
