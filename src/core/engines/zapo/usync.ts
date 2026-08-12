import { WA_DEFAULTS, WA_IQ_TYPES, WA_NODE_TAGS, WA_XMLNS } from 'zapo-js';

/**
 * zapo has no coordinator for "is this phone number on WhatsApp?", so the
 * engine issues the usync IQ itself through client.lowlevel.query().
 *
 * The envelope mirrors zapo's own internal builder. Its helpers live behind
 * subpath exports (zapo-js/transport, zapo-js/protocol) that this project's
 * moduleResolution cannot resolve, so the few constants that are not on the
 * package root are inlined below - they are protocol values, fixed by
 * WhatsApp, not zapo's own settings.
 */
const USYNC_INDEX = '0';
const USYNC_LAST = 'true';
const USYNC_MODE = 'query';
const USYNC_CONTEXT = 'interactive';

interface Node {
  tag: string;
  attrs: Record<string, string>;
  content?: any;
}

/**
 * The contact protocol identifies the peer by phone number carried as the
 * text of a <contact> child, not by a jid attribute - the number is exactly
 * what is not yet known to be a WhatsApp account. zapo's own user-node builder
 * sets the jid attribute because its usync callers already hold one.
 */
export function buildContactUsyncIq(sid: string, phones: string[]): Node {
  const users = phones.map((phone) => ({
    tag: WA_NODE_TAGS.USER,
    attrs: {},
    content: [
      {
        tag: WA_NODE_TAGS.CONTACT,
        attrs: {},
        content: `+${phone.replace(/\D/g, '')}`,
      },
    ],
  }));
  return {
    tag: WA_NODE_TAGS.IQ,
    attrs: {
      to: WA_DEFAULTS.HOST_DOMAIN,
      type: WA_IQ_TYPES.GET,
      xmlns: WA_XMLNS.USYNC,
    },
    content: [
      {
        tag: WA_NODE_TAGS.USYNC,
        attrs: {
          sid: sid,
          index: USYNC_INDEX,
          last: USYNC_LAST,
          mode: USYNC_MODE,
          context: USYNC_CONTEXT,
        },
        content: [
          {
            tag: WA_NODE_TAGS.QUERY,
            attrs: {},
            content: [{ tag: WA_NODE_TAGS.CONTACT, attrs: {} }],
          },
          { tag: WA_NODE_TAGS.LIST, attrs: {}, content: users },
        ],
      },
    ],
  };
}

function findChild(node: any, tag: string): Node | null {
  if (!Array.isArray(node?.content)) {
    return null;
  }
  return node.content.find((child: Node) => child?.tag === tag) ?? null;
}

function findChildren(node: any, tag: string): Node[] {
  if (!Array.isArray(node?.content)) {
    return [];
  }
  return node.content.filter((child: Node) => child?.tag === tag);
}

export interface UsyncContactResult {
  jid: string | null;
  exists: boolean;
}

/**
 * Reads back the `<user jid="..."><contact type="in|out"/></user>` entries.
 * `type="in"` means the number is registered on WhatsApp.
 */
export function parseContactUsyncResult(result: any): UsyncContactResult[] {
  const usyncNode = findChild(result, WA_NODE_TAGS.USYNC);
  const listNode = findChild(usyncNode, WA_NODE_TAGS.LIST);
  if (!listNode) {
    return [];
  }
  return findChildren(listNode, WA_NODE_TAGS.USER).map((user) => {
    const contact = findChild(user, WA_NODE_TAGS.CONTACT);
    return {
      jid: user?.attrs?.jid ?? null,
      exists: contact?.attrs?.type === 'in',
    };
  });
}
