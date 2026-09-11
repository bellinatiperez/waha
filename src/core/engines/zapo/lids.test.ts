import { NotFoundException } from '@nestjs/common';

import { WhatsappSessionZapoCore } from './session.zapo.core';

/**
 * Exercises LID <-> phone number resolution and the inbound payload
 * enrichment without booting a session: both read only the message key or a
 * stubbed contact store.
 */
class SessionUnderTest {
  findPNByLid = WhatsappSessionZapoCore.prototype['findPNByLid'];
  findLIDByPhoneNumber = WhatsappSessionZapoCore.prototype['findLIDByPhoneNumber'];
  toWAMessage = WhatsappSessionZapoCore.prototype['toWAMessage'];
  pnFromKey = WhatsappSessionZapoCore.prototype['pnFromKey'];
  extractText = WhatsappSessionZapoCore.prototype['extractText'];
  ensureSuffix = WhatsappSessionZapoCore.prototype['ensureSuffix'];

  contacts: any = { records: new Map(), phones: new Map() };

  sessionStores() {
    return { contacts: this.contacts };
  }
}

function buildSession(): any {
  const session = new SessionUnderTest();
  session.contacts = {
    getByJid: (jid: string) => Promise.resolve(session.contacts.records.get(jid) ?? null),
    getByPhoneNumber: (pn: string) =>
      Promise.resolve(session.contacts.phones.get(pn) ?? null),
    records: new Map<string, any>(),
    phones: new Map<string, any>(),
  };
  return session;
}

describe('ZAPO findPNByLid', () => {
  it('resolves a known LID to the phone number in @c.us form', async () => {
    const session = buildSession();
    session.contacts.records.set('254777627828362@lid', {
      jid: '254777627828362@lid',
      phoneNumber: '5511999999999@s.whatsapp.net',
    });

    const result = await session.findPNByLid('254777627828362@lid');

    expect(result).toEqual({
      lid: '254777627828362@lid',
      pn: '5511999999999@c.us',
    });
  });

  it('throws NotFound (404) for a LID unknown to the session', async () => {
    const session = buildSession();
    await expect(session.findPNByLid('111@lid')).rejects.toBeInstanceOf(
      NotFoundException,
    );
  });

  it('throws NotFound when the contact exists but carries no phone number', async () => {
    const session = buildSession();
    session.contacts.records.set('222@lid', { jid: '222@lid' });
    await expect(session.findPNByLid('222@lid')).rejects.toBeInstanceOf(
      NotFoundException,
    );
  });
});

describe('ZAPO findLIDByPhoneNumber', () => {
  it('resolves a known phone number to its LID', async () => {
    const session = buildSession();
    session.contacts.phones.set('5511999999999@s.whatsapp.net', {
      jid: '254777627828362@lid',
      lid: '254777627828362@lid',
      phoneNumber: '5511999999999@s.whatsapp.net',
    });

    const result = await session.findLIDByPhoneNumber('5511999999999@c.us');

    expect(result).toEqual({
      lid: '254777627828362@lid',
      pn: '5511999999999@c.us',
    });
  });

  it('returns lid=null (200, not 404) when the phone number has no known LID', async () => {
    const session = buildSession();
    const result = await session.findLIDByPhoneNumber('5511888888888@c.us');
    expect(result).toEqual({ lid: null, pn: '5511888888888@c.us' });
  });
});

describe('ZAPO inbound payload senderPn enrichment', () => {
  it('exposes the sender phone number for a 1:1 message addressed by LID', () => {
    const session = buildSession();
    const message = session.toWAMessage({
      key: {
        id: 'AAA',
        remoteJid: '254777627828362@lid',
        remoteJidAlt: '5511999999999@s.whatsapp.net',
        fromMe: false,
      },
      timestampSeconds: 1700000000,
      message: { conversation: 'hi' },
    });

    // `from` stays in the addressed (@lid) form for compatibility
    expect(message.from).toBe('254777627828362@lid');
    expect(message.senderPn).toBe('5511999999999@c.us');
  });

  it('exposes the participant phone number for a group message addressed by LID', () => {
    const session = buildSession();
    const message = session.toWAMessage({
      key: {
        id: 'BBB',
        remoteJid: '120@g.us',
        participant: '254777627828362@lid',
        participantAlt: '5511999999999@s.whatsapp.net',
        isGroup: true,
        fromMe: false,
      },
      timestampSeconds: 1700000000,
      message: { conversation: 'hi group' },
    });

    expect(message.senderPn).toBe('5511999999999@c.us');
  });

  it('uses the primary jid when the sender is already addressed by phone number', () => {
    const session = buildSession();
    const message = session.toWAMessage({
      key: {
        id: 'CCC',
        remoteJid: '5511999999999@s.whatsapp.net',
        fromMe: false,
      },
      timestampSeconds: 1700000000,
      message: { conversation: 'pn addressed' },
    });

    expect(message.senderPn).toBe('5511999999999@c.us');
  });

  it('leaves senderPn null when no phone-number addressing is available', () => {
    const session = buildSession();
    const message = session.toWAMessage({
      key: {
        id: 'DDD',
        remoteJid: '254777627828362@lid',
        fromMe: false,
      },
      timestampSeconds: 1700000000,
      message: { conversation: 'lid only' },
    });

    expect(message.senderPn).toBeNull();
  });
});
