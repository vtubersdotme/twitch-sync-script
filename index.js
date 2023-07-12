require("dotenv").config();
const { ApiClient } = require("@twurple/api");
const { RefreshingAuthProvider } = require("@twurple/auth");
const {
  EventSubHttpListener,
  ReverseProxyAdapter,
} = require("@twurple/eventsub-http");
const { NgrokAdapter } = require("@twurple/eventsub-ngrok");
const { createPool } = require("mariadb");
const cron = require("node-cron");
const pino = require("pino");
const logger = pino(
  pino.destination({
    sync: false, // Asynchronous logging
  })
);

const clientId = process.env.TWITCH_API_ID;
const clientSecret = process.env.TWITCH_API_KEY;

(async () => {
  logger.info("Started vTubers.Me Twitch Sync Script");

  const pool = await configureMariaDB();
  const authProvider = await configureAuthProvider(pool);
  const apiClient = await configureApiClient(authProvider);

  // This is necessary to prevent conflict errors resulting from ngrok assigning a new host name every time
  // const eventSubHttpListener = await configureDevEventSubHttpListener(apiClient);
  // try {
  //   logger.info("Deleting existing event sub subscriptions");
  //   await apiClient.eventSub.deleteAllSubscriptions();
  // } catch (err) {
  //   logger.error(`Could not delete all event sub subscriptions`, err);
  // }

  const eventSubHttpListener = await configureEventSubHttpListener(apiClient);

  twitchMain(apiClient, authProvider, eventSubHttpListener, pool);

  // schedule token update job to run every 30 minutes
  cron.schedule("*/30 * * * *", async () => {
    logger.info("Running twitch cron schedule ->");
    twitchMain(apiClient, authProvider, eventSubHttpListener, pool);
  });
})();

async function configureMariaDB() {
  let pool;

  logger.info("Connecting to MariaDB");

  try {
    pool = createPool({
      host: process.env.MARIADB_HOST,
      port: process.env.MARIADB_PORT,
      user: process.env.MARIADB_USER,
      password: process.env.MARIADB_PASSWORD,
      database: process.env.MARIADB_DATABASE,
      initializationTimeout: 1000,
      connectionLimit: 50,
    });
  } catch (err) {
    logger.error(`Could not connect to MariaDB: ${err}`);
    process.exit(1);
  }
  return pool;
}

async function configureAuthProvider(pool) {
  logger.info("Configuring/updating auth provider");
  let authProvider = new RefreshingAuthProvider({
    clientId,
    clientSecret,
    onRefresh: async (userId, newTokenData) => {
      try {
        const conn = await pool.getConnection();
        const query = `UPDATE ${process.env.MARIADB_TABLE} SET twitch_tokens = ? WHERE id = ?`;
        const values = [
          {
            accessToken: newTokenData.accessToken,
            expiresIn: newTokenData.expiresIn,
            obtainmentTimestamp: newTokenData.obtainmentTimestamp,
            refreshToken: newTokenData.refreshToken,
            scope: newTokenData.scope,
          },
          userId,
        ];
        await conn.query(query, values);
        await conn.release();
      } catch (err) {
        logger.error(`Error refreshing token data for ${userId}`);
      }
      logger.info(`Refreshed token data for ${userId}`);
    },
  });
  return authProvider;
}

async function configureApiClient(authProvider) {
  logger.info("Configuring api client");
  let apiClient = new ApiClient({
    authProvider,
    logger: {
      minLevel: "error",
    },
  });
  return apiClient;
}

async function configureDevEventSubHttpListener(apiClient) {
  logger.info("Configuring event sub http listener");
  let eventSubHttpListener = new EventSubHttpListener({
    apiClient,
    adapter: new NgrokAdapter(),
    legacySecrets: false,
    secret: process.env.TWITCH_EVENT_SUB_SECRET,
  });
  await eventSubHttpListener.start();
  return eventSubHttpListener;
}

async function configureEventSubHttpListener(apiClient) {
  logger.info("Configuring event sub http listener");
  let eventSubHttpListener = new EventSubHttpListener({
    apiClient,
    adapter: new ReverseProxyAdapter({
      hostName: `${process.env.TWITCH_EVENT_SUB_URL}`,
      port: `${process.env.TWITCH_EVENT_SUB_PORT}`,
    }),
    legacySecrets: false,
    secret: process.env.TWITCH_EVENT_SUB_SECRET,
  });
  await eventSubHttpListener.start();
  return eventSubHttpListener;
}

// Save twitch stats to database, also update active status
async function updateTwitchStats(pool, twitch_stats, active, id) {
  logger.info(`Saving twitch stats to database for ${twitch_stats?.id}`);
  let conn;
  try {
    conn = await pool.getConnection();
    const query = `UPDATE ${process.env.MARIADB_TABLE} SET twitch_stats = ? WHERE id = ?`;
    const query2 = `UPDATE ${process.env.MARIADB_TABLE} SET active = ? WHERE id = ?`;
    const values = [twitch_stats, id];
    await conn.query(query, values);
    await conn.query(query2, [active, id]);
  } catch (err) {
    logger.error(
      `Error saving twitch stats to database for ${twitch_stats?.id}`
    );
  } finally {
    if (conn) {
      conn.release();
    }
  }
}

async function twitchMain(apiClient, authProvider, eventSubHttpListener, pool) {
  let conn;
  let users;
  logger.info("Running twitch main function");

  // retrieve user tokens from database where active is 1
  logger.info("Querying all active twitch users from database");

  try {
    conn = await pool.getConnection();
    users = await conn.query(
      `SELECT * FROM ${process.env.MARIADB_TABLE} WHERE active = 1`
    );
  } catch (err) {
    logger.error(
      "Error querying active users, it is not reasonable to continue:",
      err
    );
    process.exit(1);
  } finally {
    if (conn) {
      conn.release();
    }
  }

  for (const user of users) {
    try {
      let id = await user.id;
      let twitch_info = await JSON.parse(user.twitch_info);
      let twitch_stats = await JSON.parse(user.twitch_stats);
      let twitch_tokens = await JSON.parse(user.twitch_tokens);

      // This will add the user to the authProvider and then make sure their token is always refreshed (up to date).
      try {
        logger.info(
          `Adding ${twitch_info?.id} (${twitch_info?.displayName}) to authProvider`
        );
        await authProvider.addUser(`${twitch_info.id}`, {
          accessToken: twitch_tokens.access_token,
          expiresIn: twitch_tokens.expires_in,
          obtainmentTimestamp: twitch_tokens.obtainment_timestamp,
          refreshToken: twitch_tokens.refresh_token,
          scope: twitch_tokens.scope,
        });
      } catch (err) {
        logger.info(
          `Error adding user to authProvider ${twitch_info?.displayName}:`,
          err
        );
      }

      // This listens for when a broadcaster goes online, when they do, it will attempt to save their latest stats and mark them as online in our database
      await eventSubHttpListener.onStreamOnline(
        twitch_info.id,
        async (event) => {
          logger.info(
            "Streamer went live, lets do stuff:",
            event.broadcasterDisplayName
          );
          try {
            // Get broadcaster from event
            const broadcaster = await event.getBroadcaster();
            // Get stream info
            const stream = await event.getStream();
            // Get broadcaster stats
            const followTotal = await apiClient.channels
              .getChannelFollowersPaginated(broadcaster, broadcaster)
              .getTotalCount();
            // Update stream stats
            updateTwitchStats(
              pool,
              JSON.stringify({
                game_name: stream?.gameName,
                title: stream?.title,
                tags: stream?.tagIds,
                followers: followTotal,
                subscribers: 0,
                is_live: 1,
              }),
              1,
              id
            );
          } catch (err) {
            logger.error(
              `Error listening for live event for user ${twitch_info?.displayName}:`,
              err
            );
            if (err.statusCode === 400) {
              updateTwitchStats(
                pool,
                JSON.stringify({
                  game_name: "",
                  title: "",
                  tags: "",
                  followers: 0,
                  subscribers: 0,
                  is_live: 0,
                }),
                0,
                id
              );
              logger.info(
                `Invalid refresh token found, defaulting this user in the database`
              );
            }
          }
        }
      );

      // This listens for when a broadcaster goes offline, when they do, it will attempt to save their latest stats and mark them as offline in our database
      await eventSubHttpListener.onStreamOffline(
        twitch_info.id,
        async (event) => {
          logger.info(
            "Streamer went offline, lets do stuff:",
            event.broadcasterDisplayName
          );
          try {
            // Get broadcaster from event
            const broadcaster = await event.getBroadcaster();
            // Get stream info
            const stream = await event.getStream();
            // Get broadcaster stats
            const followTotal = await apiClient.channels
              .getChannelFollowersPaginated(broadcaster, broadcaster)
              .getTotalCount();
            // Update stream stats
            updateTwitchStats(
              pool,
              JSON.stringify({
                game_name: stream?.gameName,
                title: stream?.title,
                tags: stream?.tagIds,
                followers: followTotal,
                subscribers: 0,
                is_live: 0,
              }),
              1,
              id
            );
          } catch (err) {
            logger.error(
              `Error listening for live event for user ${twitch_info?.displayName}:`,
              err
            );
            if (err.statusCode === 400) {
              updateTwitchStats(
                pool,
                JSON.stringify({
                  game_name: "",
                  title: "",
                  tags: "",
                  followers: 0,
                  subscribers: 0,
                  is_live: 0,
                }),
                0,
                id
              );
              logger.info(
                `Invalid refresh token found, defaulting this user in the database`
              );
            }
          }
        }
      );
    } catch (e) {
      logger.error(
        `Error updating subscriptions for user ${twitch_info?.displayName}:`,
        e
      );
    }
  }
}
