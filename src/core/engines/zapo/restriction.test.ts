import { ReachoutTimelockEnforcementType } from '@waha/structures/sessions.dto';

import { WhatsappSessionZapoCore } from './session.zapo.core';

function buildSession(timelock) {
  const session: any = {
    reachoutTimelock: { value: timelock },
    buildRestrictionError:
      WhatsappSessionZapoCore.prototype['buildRestrictionError'],
    timelockEndsAtIso: WhatsappSessionZapoCore.prototype['timelockEndsAtIso'],
    getRestriction: WhatsappSessionZapoCore.prototype['getRestriction'],
  };
  return session;
}

const ACTIVE = {
  isActive: true,
  enforcementType: ReachoutTimelockEnforcementType.DEFAULT,
  timeEnforcementEnds: 1784477333,
};

describe('ZAPO account restriction', () => {
  it('reports the restriction when the account carries an active timelock', () => {
    const error = buildSession(ACTIVE).buildRestrictionError(undefined);

    expect(error).toEqual({
      code: '463',
      blocked: true,
      reason: 'account_restricted',
      until: '2026-07-19T16:08:53.000Z',
      enforcementType: ReachoutTimelockEnforcementType.DEFAULT,
    });
  });

  it('trusts a 463 from the publish even before the timelock is known', () => {
    // zapo hands over the raw error code, which the other engines do not have
    // for every send - a 463 is confirmation on its own.
    const error = buildSession(null).buildRestrictionError(463);
    expect(error.code).toBe('463');
    expect(error.until).toBeNull();
  });

  it('stays quiet for an unrelated failure on an unrestricted account', () => {
    expect(buildSession(null).buildRestrictionError(401)).toBeNull();
  });

  it('exposes no restriction while the timelock is inactive', () => {
    expect(buildSession({ isActive: false }).getRestriction()).toBeNull();
  });

  it('exposes the restriction window while enforcement is on', () => {
    expect(buildSession(ACTIVE).getRestriction()).toEqual({
      active: true,
      until: '2026-07-19T16:08:53.000Z',
      enforcementType: ReachoutTimelockEnforcementType.DEFAULT,
    });
  });
});
