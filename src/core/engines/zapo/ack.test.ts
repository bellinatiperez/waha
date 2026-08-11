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
