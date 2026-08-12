import { buildContactUsyncIq, parseContactUsyncResult } from './usync';

describe('ZAPO usync contact query', () => {
  it('identifies the peer by phone text, not by a jid attribute', () => {
    const iq = buildContactUsyncIq('sid-1', ['5548991215633']);

    const usync = iq.content[0];
    const list = usync.content[1];
    const user = list.content[0];

    // A jid attribute would be wrong here: the whole point of the query is
    // that we do not know yet whether the number is a WhatsApp account.
    expect(user.attrs).toEqual({});
    expect(user.content[0].tag).toBe('contact');
    expect(user.content[0].content).toBe('+5548991215633');
  });

  it('strips punctuation from the phone number', () => {
    const iq = buildContactUsyncIq('sid-2', ['+55 (48) 99121-5633']);
    const user = iq.content[0].content[1].content[0];
    expect(user.content[0].content).toBe('+5548991215633');
  });

  it('asks for the contact protocol', () => {
    const iq = buildContactUsyncIq('sid-3', ['5511999999999']);
    const query = iq.content[0].content[0];
    expect(query.tag).toBe('query');
    expect(query.content.map((n) => n.tag)).toEqual(['contact']);
  });

  it('reads type=in as registered and anything else as not', () => {
    const result = {
      tag: 'iq',
      attrs: {},
      content: [
        {
          tag: 'usync',
          attrs: {},
          content: [
            {
              tag: 'list',
              attrs: {},
              content: [
                {
                  tag: 'user',
                  attrs: { jid: '554891215633@s.whatsapp.net' },
                  content: [{ tag: 'contact', attrs: { type: 'in' } }],
                },
                {
                  tag: 'user',
                  attrs: { jid: '5511000000000@s.whatsapp.net' },
                  content: [{ tag: 'contact', attrs: { type: 'out' } }],
                },
              ],
            },
          ],
        },
      ],
    };

    expect(parseContactUsyncResult(result)).toEqual([
      { jid: '554891215633@s.whatsapp.net', exists: true },
      { jid: '5511000000000@s.whatsapp.net', exists: false },
    ]);
  });

  it('returns nothing when the result carries no usync payload', () => {
    expect(parseContactUsyncResult({ tag: 'iq', attrs: {} })).toEqual([]);
  });
});
