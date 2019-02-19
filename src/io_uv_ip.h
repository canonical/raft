/* IP-related utils. */

#ifndef RAFT_IO_UV_IP_H_
#define RAFT_IO_UV_IP_H_

#include <netinet/in.h>

/**
 * Split @address into @host and @port and populate @addr accordingly.
 */
int raft__io_uv_ip_parse(const char *address, struct sockaddr_in *addr);

#endif /* RAFT_IO_UV_IP_H */
