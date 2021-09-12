// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const tor = @import("tor.zig");
const scan = @import("scan.zig");
const util = @import("util.zig");
const repometa = @import("repometa.zig");
const config = @import("config.zig");

fn serverAddress() std.net.Address {
    // Localhost IPv4 address if we can't use a UNIX address.
    // There's no real meaning behind the port number. It should be
    // automatically assigned, actually, but then we need to store the port
    // somewhere and it makes the daemon-already-running check more fragile.
    const ipv4 = std.net.Address.initIp4(.{127,0,0,1}, 11649);

    if (std.net.has_unix_sockets) {
        var p = util.Path{};
        p.push(config.store_path) catch return ipv4;
        p.push("daemon-socket") catch return ipv4;
        return std.net.Address.initUnix(p.slice()) catch ipv4;
    }
    return ipv4;
}

pub const Client = struct {
    conn: std.net.Stream,

    const Self = @This();

    pub fn connect() !Self {
        return Client{
            // Pedantry: tcpConnectToAddress is misnamed, it also accepts UNIX sockets.
            .conn = try std.net.tcpConnectToAddress(serverAddress()),
        };
        // TODO: Do we want an extra token-stored-in-a-file authentication step for IPv4 sockets?
    }

    pub fn close(self: *Self) void {
        self.conn.close();
    }
};


// Status info of our repository
const repo = struct {
    var onion = false; // Whether our .onion address has been registered with our Tor instance.

    // TODO: Actually have a repository server (and HTTP server, optionally)

    fn start() !void {
        if (!onion) {
            try (try tor.control()).setupOnion(1234, 4532);
            onion = true;
        }
    }

    fn stop() !void {
        if (onion) {
            try (try tor.control()).removeOnion();
            onion = false;
        }
    }
};


fn daemonClient(sock: std.net.Stream) void {
    // TODO: Accept commands and stuff
    sock.close();
}

fn runServer(sock_: std.net.StreamServer) void {
    var sock = sock_;
    while (sock.accept()) |conn| {
        std.log.debug("New connection on daemon socket", .{});
        var thread = std.Thread.spawn(.{}, daemonClient, .{ conn.stream }) catch |e| {
            std.log.crit("Unable to spawn thread for new connection: {}", .{ e });
            conn.stream.close();
            continue;
        };
        thread.setName("Daemon client") catch {};
        thread.detach();
    } else |e| {
        std.log.crit("Error accepting new connection: {}; stopping daemon.", .{ e });
        std.process.exit(1);
    }
}


pub fn run() !void {
    // Yes, I know, this check is subject to a race condition. Not much we can
    // do about that other than go for a file locking mechanism or something.
    if (Client.connect()) |*c| {
        c.close();
        return error.DaemonAlreadyRunning;
    } else |_| {}

    if (serverAddress().any.family == std.os.AF.UNIX)
        std.fs.deleteFileAbsoluteZ(std.mem.sliceTo(std.meta.assumeSentinel(&serverAddress().un.path, 0), 0)) catch {};

    var server = std.net.StreamServer.init(.{ .reuse_address = true });
    defer server.deinit();
    try server.listen(serverAddress());
    std.log.info("Daemon listening on {}", .{ serverAddress() });
    var thread = try std.Thread.spawn(.{}, runServer, .{ server });
    thread.setName("Daemon server") catch {};
    thread.detach();

    while (true) {
        if (config.published_dirs.count() > 0) {
            repo.start() catch |e| {
                std.log.warn("Error starting repository server: {}", .{ e });
                continue;
            };
            std.log.info("Initiating repository refresh", .{});
            try scan.scan();
            try scan.hash();
            try repometa.write();

        } else {
            repo.stop() catch |e| {
                std.log.warn("Error stopping repository server: {}", .{ e });
                continue;
            };
        }
        std.time.sleep(std.time.ns_per_hour); // should be configurable
    }
}
