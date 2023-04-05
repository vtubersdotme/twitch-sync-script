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

const clientId = process.env.TWITCH_API_ID;
const clientSecret = process.env.TWITCH_API_KEY;

(async () => {
  console.log("Started vTubers.Me Twitch Script :3");
  const pool = await configureMariaDB();
  const authProvider = await configureAuthProvider(pool);
  const apiClient = await configureApiClient(authProvider);

  // This is necessary to prevent conflict errors resulting from ngrok assigning a new host name every time
  // const eventSubHttpListener = await configureDevEventSubHttpListener(apiClient);
  // try {
  //   console.info("Deleting existing event sub subscriptions");
  //   await apiClient.eventSub.deleteAllSubscriptions();
  // } catch (err) {
  //   console.error(`Could not delete all event sub subscriptions`, err);
  // }
  const eventSubHttpListener = await configureEventSubHttpListener(apiClient);

  twitchMain(apiClient, authProvider, eventSubHttpListener, pool);

  // schedule token update job to run every 30 minutes
  cron.schedule("*/30 * * * *", async () => {
    console.log("Running twitch cron schedule ->");
    twitchMain(apiClient, authProvider, eventSubHttpListener, pool);
  });
})();

async function configureMariaDB() {
  let pool;
  console.info("Connecting to MariaDB");
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
    console.error(`Could not connect to MariaDB: ${err}`);
    process.exit(1);
  }
  return pool;
}

async function configureAuthProvider(pool) {
  console.info("Configuring/updating auth provider");
  let authProvider = new RefreshingAuthProvider({
    clientId,
    clientSecret,
    onRefresh: async (userId, newTokenData) => {
      const conn = await pool.getConnection();
      await conn.query(
        `UPDATE ${process.env.MARIADB_TABLE} SET twitch_tokens = ? WHERE id = ${userId}`,
        [
          {
            accessToken: newTokenData.accessToken,
            expiresIn: newTokenData.expiresIn,
            obtainmentTimestamp: newTokenData.obtainmentTimestamp,
            refreshToken: newTokenData.refreshToken,
            scope: newTokenData.scope,
          },
        ]
      );
      conn.release();
    },
  });
  return authProvider;
}

async function configureApiClient(authProvider) {
  console.info("Configuring api client");
  let apiClient = new ApiClient({
    authProvider,
    logger: {
      minLevel: "error",
    },
  });
  return apiClient;
}

async function configureDevEventSubHttpListener(apiClient) {
  console.info("Configuring event sub http listener");
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
  console.info("Configuring event sub http listener");
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

async function twitchMain(apiClient, authProvider, eventSubHttpListener, pool) {
  console.info("Running twitch main function");
  // retrieve user tokens from database where active is 1
  const conn = await pool.getConnection();
  const users = await conn.query(
    `SELECT * FROM ${process.env.MARIADB_TABLE} WHERE active = 1`
  );
  conn.release();

  for (const user of users) {
    try {
      let id = await user.id;
      let twitch_info = await JSON.parse(user.twitch_info);
      let twitch_stats = await JSON.parse(user.twitch_stats);
      let twitch_tokens = await JSON.parse(user.twitch_tokens);

      // This will add the user to our authProvider and then make sure their token is always refreshed (up to date).
      try {
        console.log(
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
        console.info(
          `Error adding user to authProvider ${twitch_info?.displayName}:`,
          err
        );
      }

      // This listens for when a broadcaster goes online, when they do, it will attempt to save their latest stats and mark them as online in our database
      await eventSubHttpListener.onStreamOnline(
        twitch_info.id,
        async (event) => {
          try {
            // handle stream going online event
            const broadcaster = await event.getBroadcaster();

            // get broadcaster stats
            const followTotal = await apiClient.channels
              .getChannelFollowersPaginated(broadcaster, broadcaster)
              .getTotalCount();
            // Connect to DB and save follow/live stats
            try {
              const conn = await pool.getConnection();
              await conn.query(
                `UPDATE ${process.env.MARIADB_TABLE} SET twitch_stats = ? WHERE id = ?`,
                [
                  JSON.stringify({
                    followers: followTotal,
                    subscribers: 0,
                    is_live: 1,
                  }),
                  id,
                ]
              );
              conn.release();
              console.info(
                `Successfully saved stats for ${twitch_info?.displayName} to DB.`
              );
            } catch (err) {
              console.error(
                `Error saving follow stats for ${twitch_info?.displayName} to DB:`,
                err
              );
            }
            console.info("Stream went live:", event.broadcasterDisplayName);
          } catch (err) {
            console.error(
              `Error listening for live event for user ${twitch_info?.displayName}:`,
              err
            );
            if (err.statusCode === 400) {
              const conn = await pool.getConnection();
              await conn.query(
                `UPDATE ${process.env.MARIADB_TABLE} SET twitch_tokens = ? WHERE id = ?`,
                [
                  JSON.stringify({
                    access_token: "0",
                    refresh_token: "0",
                    expires_in: "0",
                  }),
                  id,
                ]
              );
              await conn.query(
                `UPDATE ${process.env.MARIADB_TABLE} SET active = ? WHERE id = ?`,
                [0, id]
              );
              conn.release();
              console.info(
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
          try {
            // handle stream going offline event
            const broadcaster = await event.getBroadcaster();
            // get broadcaster stats
            const followTotal = await apiClient.channels
              .getChannelFollowersPaginated(broadcaster, broadcaster)
              .getTotalCount();
            // Connect to DB and save follow stats
            try {
              const conn = await pool.getConnection();
              await conn.query(
                `UPDATE ${process.env.MARIADB_TABLE} SET twitch_stats = ? WHERE id = ?`,
                [
                  JSON.stringify({
                    followers: followTotal,
                    subscribers: 0,
                    is_live: 0,
                  }),
                  id,
                ]
              );
              conn.release();
              console.info(
                `Successfully saved stats for ${twitch_info?.displayName} to DB.`
              );
            } catch (err) {
              console.error(
                `Error saving follow stats for ${twitch_info?.displayName} to DB:`,
                err
              );
            }
            console.info("Stream went offline:", event.broadcasterDisplayName);
          } catch (err) {
            console.error(
              `Error listening for live event for user ${twitch_info?.displayName}:`,
              err
            );
            if (err.statusCode === 400) {
              const conn = await pool.getConnection();
              await conn.query(
                `UPDATE ${process.env.MARIADB_TABLE} SET twitch_tokens = ? WHERE id = ?`,
                [
                  JSON.stringify({
                    access_token: "0",
                    refresh_token: "0",
                    expires_in: "0",
                  }),
                  id,
                ]
              );
              await conn.query(
                `UPDATE ${process.env.MARIADB_TABLE} SET active = ? WHERE id = ?`,
                [0, id]
              );
              conn.release();
              console.log(
                `Invalid refresh token found, defaulting this user in the database`
              );
            }
          }
        }
      );
    } catch (e) {
      console.error(
        `Error updating subscriptions for user ${twitch_info?.displayName}:`,
        e
      );

      // If for whatever reason, we are unable to update the subscription for a user and the statuscode is 400 (bad request), this typically means we no longer have access to them and should update our database as such.
      // if (e.statusCode === 400) {
      //   const conn = await pool.getConnection();
      //   await conn.query(
      //     `UPDATE ${process.env.MARIADB_TABLE} SET twitch_tokens = ? WHERE id = ?`,
      //     [
      //       JSON.stringify({
      //         access_token: "0",
      //         refresh_token: "0",
      //         expires_in: "0",
      //       }),
      //       id,
      //     ]
      //   );
      //   await conn.query(
      //     `UPDATE ${process.env.MARIADB_TABLE} SET active = ? WHERE id = ?`,
      //     [0, id]
      //   );
      //   conn.release();
      //   console.log(
      //     `Invalid refresh token for found, defaulting this user in the database`
      //   );
      // }
    }
  }

  // eventSubHttpListener.onSubscriptionCreateSuccess(async () => {
  //   console.info("Successfully created a subscription");
  // });
  // eventSubHttpListener.onSubscriptionDeleteFailure(async () => {
  //   console.error("Failed to delete a subscription");
  // });
  // eventSubHttpListener.onSubscriptionDeleteSuccess(async () => {
  //   console.info("Successfully deleted a subscription");
  // });
  // eventSubHttpListener.onRevoke(async () => {
  //   console.info("Subscription was revoked");
  // });
}
