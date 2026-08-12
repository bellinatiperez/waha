import { WAMessageAck } from '@waha/structures/enums.dto';

import { WhatsappSessionZapoCore } from './session.zapo.core';

/**
 * Exercises the receipt -> ack mapping without booting a session: the
 * conversion is pure, only reading "me" off the session.
 */
class SessionUnderTest {
  toMessageAcks = WhatsappSessionZapoCore.prototype['toMessageAcks'];
  buildAckBody = WhatsappSessionZapoCore.prototype['buildAckBody'];
  emitSentAck = WhatsappSessionZapoCore.prototype['emitSentAck'];

  sent = [];
  sentAcks$ = { next: (body) => this.sent.push(body) };

  getSessionMeInfo() {
    return { id: '551199@s.whatsapp.net' };
  }
}

function buildSession(): any {
  return new SessionUnderTest();
}

function receipt(status: string, extra: any = {}) {
  return {
    status: status,
    chatJid: '5548999@s.whatsapp.net',
    fromSelfDevice: false,
    messageIds: ['AAA'],
    ...extra,
  };
}

describe('ZAPO acks', () => {
  it.each([
    ['delivered', WAMessageAck.DEVICE, 'DEVICE'],
    ['read', WAMessageAck.READ, 'READ'],
    ['played', WAMessageAck.PLAYED, 'PLAYED'],
  ])('maps the %s receipt to %s', (status, ack, ackName) => {
    const session = buildSession();
    const bodies = session.toMessageAcks(receipt(status));

    expect(bodies).toHaveLength(1);
    expect(bodies[0].ack).toBe(ack);
    expect(bodies[0].ackName).toBe(ackName);
    expect(bodies[0].id).toBe('AAA');
    expect(bodies[0].fromMe).toBe(true);
  });

  it('drops the inactive receipt, it is a presence hint and not an ack', () => {
    const session = buildSession();
    expect(session.toMessageAcks(receipt('inactive'))).toEqual([]);
  });

  it('fans a batch receipt out into one ack per message id', () => {
    const session = buildSession();
    const bodies = session.toMessageAcks(
      receipt('read', { messageIds: ['A', 'B', 'C'] }),
    );
    expect(bodies.map((body) => body.id)).toEqual(['A', 'B', 'C']);
  });

  it('falls back to the stanza id when the receipt carries no list', () => {
    const session = buildSession();
    const bodies = session.toMessageAcks(
      receipt('delivered', { messageIds: [], stanzaId: 'STANZA' }),
    );
    expect(bodies.map((body) => body.id)).toEqual(['STANZA']);
  });

  it('marks a receipt from our own device as acking an inbound message', () => {
    const session = buildSession();
    const bodies = session.toMessageAcks(
      receipt('read', { fromSelfDevice: true }),
    );
    expect(bodies[0].fromMe).toBe(false);
    // from/to swap with the direction
    expect(bodies[0].from).toBe('5548999@c.us');
    expect(bodies[0].to).toBe('551199@c.us');
  });

  it('emits SERVER for a published message', () => {
    const session = buildSession();
    session.emitSentAck('5548999@s.whatsapp.net', { id: 'X1', ack: {} });

    expect(session.sent).toHaveLength(1);
    expect(session.sent[0].ack).toBe(WAMessageAck.SERVER);
    expect(session.sent[0].ackName).toBe('SERVER');
    expect(session.sent[0].fromMe).toBe(true);
  });

  it('emits ERROR when WhatsApp rejected the publish', () => {
    const session = buildSession();
    session.emitSentAck('5548999@s.whatsapp.net', {
      id: 'X2',
      ack: { error: 403 },
    });

    expect(session.sent[0].ack).toBe(WAMessageAck.ERROR);
    expect(session.sent[0].ackName).toBe('ERROR');
    expect(session.sent[0]._data.error).toBe(403);
  });
});

describe('ZAPO ack destination', () => {
  it('reuses the published chat so later acks match the sent ack', () => {
    const session = buildSession();
    // The publish path recorded where the message actually went...
    session.sentChats = { get: (id) => (id === 'AAA' ? '5548999@c.us' : null) };

    // ...while the receipt comes back addressed by LID.
    const bodies = session.toMessageAcks(
      receipt('delivered', { chatJid: '254777627828362@lid' }),
    );

    expect(bodies[0].to).toBe('5548999@c.us');
  });

  it('falls back to the receipt jid for messages it did not publish', () => {
    const session = buildSession();
    session.sentChats = { get: () => null };

    const bodies = session.toMessageAcks(
      receipt('read', { fromSelfDevice: true }),
    );
    expect(bodies[0].from).toBe('5548999@c.us');
  });
});

describe('ZAPO me info', () => {
  function sessionWithCredentials(credentials) {
    const session: any = {
      buildMeInfo: WhatsappSessionZapoCore.prototype['buildMeInfo'],
      client: { getCredentials: () => credentials },
    };
    return session;
  }

  it('reports the plain chat id, keeping the device jid apart', () => {
    const me = sessionWithCredentials({
      meJid: '554891600684:80@s.whatsapp.net',
      meLid: '13392429502664:80@lid',
      meDisplayName: 'Cleber',
    }).buildMeInfo();

    // A consumer comparing me.id against a chat id has to find a match.
    expect(me.id).toBe('554891600684@c.us');
    expect(me.jid).toBe('554891600684:80@s.whatsapp.net');
    expect(me.lid).toBe('13392429502664@lid');
    expect(me.pushName).toBe('Cleber');
  });

  it('returns null before the session is paired', () => {
    expect(sessionWithCredentials(null).buildMeInfo()).toBeNull();
  });
});

describe('ZAPO qr status', () => {
  function sessionOnQr(initial: string, paired: boolean) {
    const emitted: string[] = [];
    let current = initial;
    let handler: (event: { qr: string }) => void;
    const session: any = {
      paired: paired,
      qr: { save: () => undefined },
      printQR: () => undefined,
      logger: { debug: () => undefined },
      client: {
        on: (event: string, fn: any) => {
          if (event === 'auth_qr') {
            handler = fn;
          }
        },
      },
      setStatus: () => undefined,
    };
    Object.defineProperty(session, 'status', {
      get: () => current,
      set: (value: string) => {
        current = value;
        emitted.push(value);
      },
    });

    WhatsappSessionZapoCore.prototype['listenAuthEvents'].call(session);
    handler({ qr: 'raw-qr' });
    return emitted;
  }

  it('does not re-issue the status on every QR rotation', () => {
    // The QR refreshes every few seconds - each one would be a webhook.
    expect(sessionOnQr('SCAN_QR_CODE', false)).toEqual([]);
  });

  it('issues SCAN_QR_CODE the first time', () => {
    expect(sessionOnQr('STARTING', false)).toEqual(['SCAN_QR_CODE']);
  });

  it('ignores a QR refresh once the session is paired', () => {
    // zapo keeps rotating the QR while it reconnects after pairing; bouncing
    // back to SCAN_QR_CODE would cancel the pending WORKING event.
    expect(sessionOnQr('WORKING', true)).toEqual([]);
  });
});
