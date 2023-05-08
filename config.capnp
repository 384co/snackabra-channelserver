# Imports the base schema for workerd configuration files.

# Refer to the comments in /src/workerd/server/workerd.capnp for more details.

using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (

  services = [ (name = "main", worker = .snackabra) ],

  sockets = [
    ( name = "http", address = "*:4001", http = (), service = "main" ),
    ( name = "ws", address = "*:4002", http = (), service = "main" )
   ]
);

const snackabra :Workerd.Worker = (

  durableObjectNamespaces = [
    # uniqueKey doesn't matter (for us) just needs to be unique
    (className = "ChannelServer", uniqueKey = "e5Bkote5Bkote5Bkote5Bkote5Bkote5")
  ],

  durableObjectStorage = (inMemory = void),

  modules = [
    (name = "worker", esModule = embed "src/index.js"),
    (name = "snackabra.js", esModule = embed "src/snackabra.js"),
  ],

  # need these to be able to call back to our own DO namespace;
  # they will show up on the 'env' object past with 'fetch()'
  bindings = [
    (name = "rooms", durableObjectNamespace = "ChatRoomAPI"),
    (name = "MESSAGES_NAMESPACE", kvNamespace = "MESSAGES_NAMESPACE"),
    (name = "KEYS_NAMESPACE", kvNamespace = "KEYS_NAMESPACE"),
    (name = "LEDGER_NAMESPACE", kvNamespace = "LEDGER_NAMESPACE"),
    (name = "IMAGES_NAMESPACE", kvNamespace = "IMAGES_NAMESPACE"),
    # (name = "RECOVERY_NAMESPACE", kvNamespace = "RECOVERY_NAMESPACE")
  ],

  # see https://developers.cloudflare.com/workers/platform/compatibility-dates/
  compatibilityDate = "2023-04-19",
);
