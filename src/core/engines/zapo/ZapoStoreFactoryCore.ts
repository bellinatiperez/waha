import { DataStore } from '@waha/core/abc/DataStore';
import { LocalStore } from '@waha/core/storage/LocalStore';
import { MongoStore } from '@waha/core/storage/mongo/MongoStore';
import { PsqlStore } from '@waha/core/storage/psql/PsqlStore';
import { createMongoStore, WaMongoStoreResult } from '@zapo-js/store-mongo';
import { createPostgresStore, WaPgStoreResult } from '@zapo-js/store-postgres';
import { createSqliteStore, WaSqliteStoreResult } from '@zapo-js/store-sqlite';
import { createStore, WaStore } from 'zapo-js';

// The file lives next to the other engine data for the session, so backing up
// (or mounting) the sessions folder keeps working the same way as before.
export const ZAPO_SQLITE_FILE = 'zapo.sqlite';

type ZapoBackend = WaSqliteStoreResult | WaPgStoreResult | WaMongoStoreResult;

/**
 * zapo requires EVERY persistence domain to be assigned explicitly once
 * "backends" is set - createStore throws listing the missing ones otherwise.
 * Cache domains (retry, groupMetadata, chatMetadata, deviceList,
 * messageSecret) stay on the in-memory defaults on purpose: they are small,
 * hot and cheap to rebuild.
 */
const PROVIDERS = {
  auth: 'waha',
  signal: 'waha',
  preKey: 'waha',
  session: 'waha',
  identity: 'waha',
  senderKey: 'waha',
  appState: 'waha',
  privacyToken: 'waha',
  messages: 'waha',
  threads: 'waha',
  contacts: 'waha',
} as const;

/**
 * Builds the zapo store on top of whatever storage WAHA is configured with,
 * so the operator keeps a single storage configuration for every engine.
 *
 * Mirrors NowebAuthFactoryCore: the WAHA DataStore decides the backend.
 */
export class ZapoStoreFactoryCore {
  buildStore(store: DataStore, name: string): WaStore {
    if (store instanceof MongoStore) {
      return this.buildMongoStore(store, name);
    }
    if (store instanceof PsqlStore) {
      return this.buildPsqlStore(store, name);
    }
    if (store instanceof LocalStore) {
      return this.buildLocalStore(store, name);
    }
    throw new Error(`Unsupported store type '${store.constructor.name}'`);
  }

  protected buildMongoStore(store: MongoStore, name: string): WaStore {
    const db = store.getSessionDb(name);
    const backend = createMongoStore({ db: db });
    return this.createStore(backend);
  }

  protected buildPsqlStore(store: PsqlStore, name: string): WaStore {
    // Reuses the per-session database WAHA already provisions for the session.
    const connectionString = store.getSessionDbURL(name);
    const backend = createPostgresStore({
      pool: { connectionString: connectionString },
    });
    return this.createStore(backend);
  }

  protected buildLocalStore(store: LocalStore, name: string): WaStore {
    const path = store.getFilePath(name, ZAPO_SQLITE_FILE);
    const backend = createSqliteStore({ path: path });
    return this.createStore(backend);
  }

  protected createStore(backend: ZapoBackend): WaStore {
    const options = {
      backends: { waha: backend },
      providers: PROVIDERS,
    };
    // zapo resolves which backend may serve each domain with
    // `undefined extends TBackends[K][Kind][D]`, which is always true while
    // strictNullChecks is off (this project's tsconfig) - every domain then
    // collapses to accepting only 'memory'. The cast restores the intended
    // call; createStore still validates the domain coverage at runtime.
    return createStore(options as any);
  }
}
