---
title: FAQ
nav_order: 1
permalink: /faq
---

# FAQ

## Why does Direxio Message Server exist?

Direxio Message Server aims to provide a matrix compatible server that has low resource usage compared to [Synapse](https://github.com/matrix-org/synapse).
It also aims to provide more flexibility when scaling either up or down.
Direxio Message Server's code is also very easy to hack on which makes it suitable for experimenting with new matrix features such as peer-to-peer.

## Is Direxio Message Server stable?

Mostly, although there are still bugs and missing features. If you are a confident power user and you are happy to spend some time debugging things when they go wrong, then please try out Direxio Message Server. If you are a community, organisation or business that demands stability and uptime, then Direxio Message Server is not for you yet - please install Synapse instead.

## Is Direxio Message Server feature-complete?

No, although a good portion of the Matrix specification has been implemented. Mostly missing are client features - see the [readme](https://github.com/YingSuiAI/direxio-message-server/blob/main/README.md) at the root of the repository for more information.

## Why doesn't Direxio Message Server have "x" yet?

Direxio Message Server development is currently supported by a small team of developers and due to those limited resources, the majority of the effort is focused on getting Direxio Message Server to be
specification complete. If there are major features you're requesting (e.g. new administration endpoints), we'd like to strongly encourage you to join the community in supporting
the development efforts through [contributing](./development/CONTRIBUTING.md).

## Is there a migration path from Synapse to Direxio Message Server?

No, not at present. There will be in the future when Direxio Message Server reaches version 1.0. For now it is not
possible to migrate an existing Synapse deployment to Direxio Message Server.

## Can I use Direxio Message Server with an existing Synapse database?

No, Direxio Message Server has a very different database schema to Synapse and the two are not interchangeable.

## Can I configure which port Direxio Message Server listens on?

Yes, use the cli flag `-http-bind-address`.

## I've installed Direxio Message Server but federation isn't working

Check the [Federation Tester](https://federationtester.matrix.org). You need at least:

* A valid DNS name
* A valid TLS certificate for that DNS name
* Either DNS SRV records or well-known files

## Whenever I try to connect from Element it says unable to connect to homeserver

Check that your dendrite instance is running. Otherwise this is most likely due to a reverse proxy misconfiguration.

## Does Direxio Message Server work with my favourite client?

It should do, although we are aware of some minor issues:

* **Element Android**: registration does not work, but logging in with an existing account does
* **Hydrogen**: occasionally sync can fail due to gaps in the `since` parameter, but clearing the cache fixes this

## Is there a public instance of Direxio Message Server I can try out?

Use [dendrite.matrix.org](https://dendrite.matrix.org) which we officially support.

## Does Direxio Message Server support Space Summaries?

Yes

## Does Direxio Message Server support Threads?

Yes, to enable them [msc2836](https://github.com/matrix-org/matrix-spec-proposals/pull/2836) would need to be added to mscs configuration in order to support Threading. Other MSCs are not currently supported.

```
mscs:
  mscs:
    - msc2836
```

Please note that MSCs should be considered experimental and can result in significant usability issues when enabled. If you'd like more details on how MSCs are ratified or the current status of MSCs, please see the [Matrix specification documentation](https://spec.matrix.org/proposals/) on the subject.

## Does Direxio Message Server support push notifications?

Yes, we have experimental support for push notifications. Configure them in the usual way in your Matrix client.

## Does Direxio Message Server support application services/bridges?

Possibly - Direxio Message Server does have some application service support but it is not well tested. Please let us know by raising a GitHub issue if you try it and run into problems.

Bridges known to work (as of v0.5.1):

* [Telegram](https://docs.mau.fi/bridges/python/telegram/index.html)
* [WhatsApp](https://docs.mau.fi/bridges/go/whatsapp/index.html)
* [Signal](https://docs.mau.fi/bridges/python/signal/index.html)
* [probably all other mautrix bridges](https://docs.mau.fi/bridges/)

Remember to add the config file(s) to the `app_service_api` section of the config file.

## Is it possible to prevent communication with the outside world?

Yes, you can do this by disabling federation - set `disable_federation` to `true` in the `global` section of the Direxio Message Server configuration file.

## How can I migrate a room in order to change the internal ID?

This can be done by performing a room upgrade. Use the command `/upgraderoom <version>` in Element to do this.

## How do I reset somebody's password on my server?

Use the admin endpoint [resetpassword](./administration/4_adminapi.md#post-_dendriteadminresetpassworduserid)

## Should I use PostgreSQL or SQLite for my databases?

Please use PostgreSQL wherever possible, especially if you are planning to run a homeserver that caters to more than a couple of users.

## What data needs to be kept if transferring/backing up Direxio Message Server?

The list of files that need to be stored is:
- matrix-key.pem
- message-server.yaml
- the postgres or sqlite DB
- the jetstream directory
- the media store
- the search index (although this can be regenerated)

Note that this list may change / be out of date. We don't officially maintain instructions for migrations like this.

## How can I prepare enough storage for media caches?

This might be what you want: [matrix-media-repo](https://github.com/turt2live/matrix-media-repo)
We don't officially support this or any other dedicated media storage solutions.

## Is there an upgrade guide for Direxio Message Server?

Run a newer docker image. We don't officially support deployments other than Docker.
Most of the time you should be able to just
- stop
- replace binary
- start

## Direxio Message Server is using a lot of CPU

Generally speaking, you should expect to see some CPU spikes, particularly if you are joining or participating in large rooms. However, constant/sustained high CPU usage is not expected - if you are experiencing that, please join `#dendrite-dev:matrix.org` and let us know what you were doing when the
CPU usage shot up, or file a GitHub issue. If you can take a [CPU profile](development/PROFILING.md) then that would
be a huge help too, as that will help us to understand where the CPU time is going.

## Direxio Message Server is using a lot of RAM

As above with CPU usage, some memory spikes are expected if Direxio Message Server is doing particularly heavy work
at a given instant. However, if it is using more RAM than you expect for a long time, that's probably
not expected. Join `#dendrite-dev:matrix.org` and let us know what you were doing when the memory usage
ballooned, or file a GitHub issue if you can. If you can take a [memory profile](development/PROFILING.md) then that
would be a huge help too, as that will help us to understand where the memory usage is happening.

## Do I need to generate the self-signed certificate if I'm going to use a reverse proxy?

No, if you already have a proper certificate from some provider, like Let's Encrypt, and use that on your reverse proxy, and the reverse proxy does TLS termination, then you’re good and can use HTTP to the dendrite process.

## Direxio Message Server is running out of PostgreSQL database connections

You may need to revisit the connection limit of your PostgreSQL server and/or make changes to the `max_connections` lines in your Direxio Message Server configuration. Be aware that each Direxio Message Server component opens its own database connections and has its own connection limit, even in monolith mode!

## VOIP and Video Calls don't appear to work on Direxio Message Server

There is likely an issue with your STUN/TURN configuration on the server. If you believe your configuration to be correct, please see the [troubleshooting](administration/6_troubleshooting.md) for troubleshooting recommendations.

## What is being reported when enabling phone-home statistics?

Phone-home statistics contain your server's domain name, some configuration information about
your deployment and aggregated information about active users on your deployment. They are sent
to the endpoint URL configured in your Direxio Message Server configuration file only. The following is an
example of the data that is sent:

```json
{
    "cpu_average": 0,
    "daily_active_users": 97,
    "daily_e2ee_messages": 0,
    "daily_messages": 0,
    "daily_sent_e2ee_messages": 0,
    "daily_sent_messages": 0,
    "daily_user_type_bridged": 2,
    "daily_user_type_native": 97,
    "database_engine": "Postgres",
    "database_server_version": "11.14 (Debian 11.14-0+deb10u1)",
    "federation_disabled": false,
    "go_arch": "amd64",
    "go_os": "linux",
    "go_version": "go1.16.13",
    "homeserver": "my.domain.com",
    "log_level": "trace",
    "memory_rss": 93452,
    "monolith": true,
    "monthly_active_users": 97,
    "nats_embedded": true,
    "nats_in_memory": true,
    "num_cpu": 8,
    "num_go_routine": 203,
    "r30v2_users_all": 0,
    "r30v2_users_android": 0,
    "r30v2_users_electron": 0,
    "r30v2_users_ios": 0,
    "r30v2_users_web": 0,
    "timestamp": 1651741851,
    "total_nonbridged_users": 97,
    "total_room_count": 0,
    "total_users": 99,
    "uptime_seconds": 30,
    "version": "0.8.2"
}
```
