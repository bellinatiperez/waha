import { LocalStoreCore } from '@waha/core/storage/LocalStoreCore';
import * as fs from 'fs/promises';
import * as os from 'os';
import * as path from 'path';

import { ZAPO_SQLITE_FILE, ZapoStoreFactoryCore } from './ZapoStoreFactoryCore';

const PERSISTENCE_DOMAINS = [
  'auth',
  'signal',
  'preKey',
  'session',
  'identity',
  'senderKey',
  'appState',
  'privacyToken',
  'messages',
  'threads',
  'contacts',
] as const;

describe('ZapoStoreFactoryCore', () => {
  const SESSION = 'default';
  let baseDir: string;
  let store: LocalStoreCore;

  beforeEach(async () => {
    baseDir = await fs.mkdtemp(path.join(os.tmpdir(), 'waha-zapo-'));
    process.env.WAHA_LOCAL_STORE_BASE_DIR = baseDir;
    store = new LocalStoreCore('waha', 'zapo');
    await store.init(SESSION);
  });

  afterEach(async () => {
    delete process.env.WAHA_LOCAL_STORE_BASE_DIR;
    await store.close();
    await fs.rm(baseDir, { recursive: true, force: true });
  });

  // Reading/writing a domain is not covered here: the sqlite backend loads
  // better-sqlite3 through a dynamic import, which jest's CommonJS runtime
  // cannot resolve. That path is exercised when the engine runs for real.
  it('builds a store for LocalStore with every persistence domain routed', async () => {
    const factory = new ZapoStoreFactoryCore();
    const zapoStore = factory.buildStore(store, SESSION);

    // Resolving a session is what validates the provider/domain coverage -
    // an unrouted domain throws here, not at createStore() time.
    const session = zapoStore.session(SESSION);
    for (const domain of PERSISTENCE_DOMAINS) {
      expect(session[domain]).toBeDefined();
    }

    await zapoStore.destroy();
  });

  it('keeps the session file inside the WAHA sessions folder', () => {
    const file = store.getFilePath(SESSION, ZAPO_SQLITE_FILE);
    expect(file.startsWith(store.getBaseDirectory())).toBe(true);
    expect(path.basename(file)).toBe(ZAPO_SQLITE_FILE);
  });

  it('rejects an unsupported store type', () => {
    const factory = new ZapoStoreFactoryCore();
    const unsupported = { constructor: { name: 'WeirdStore' } } as any;
    expect(() => factory.buildStore(unsupported, SESSION)).toThrow(
      /Unsupported store type/,
    );
  });
});
