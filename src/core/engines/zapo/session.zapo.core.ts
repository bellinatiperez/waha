import { UnprocessableEntityException } from '@nestjs/common';
import { Activity } from '@waha/core/abc/activity';
import { WhatsappSession } from '@waha/core/abc/session.abc';
import { NotImplementedByEngineError } from '@waha/core/exceptions';
import { QR } from '@waha/core/QR';
import { toJID } from '@waha/core/utils/jids';
import {
  CheckNumberStatusQuery,
  ChatRequest,
  MessageFileRequest,
  MessageForwardRequest,
  MessageImageRequest,
  MessageLocationRequest,
  MessageReactionRequest,
  MessageReplyRequest,
  MessageTextRequest,
  MessageVoiceRequest,
  SendSeenRequest,
} from '@waha/structures/chatting.dto';
import {
  ReadChatMessagesQuery,
  ReadChatMessagesResponse,
} from '@waha/structures/chats.dto';
import {
  WAHAEngine,
  WAHAEvents,
  WAHASessionStatus,
} from '@waha/structures/enums.dto';
import { WAMessage } from '@waha/structures/responses.dto';
import { PairingCodeResponse } from '@waha/structures/auth.dto';
import { MeInfo } from '@waha/structures/sessions.dto';
import { Observable } from 'rxjs';
import { filter, map } from 'rxjs/operators';
import { createMediaProcessor } from '@zapo-js/media-utils';
import { voipPlugin } from '@zapo-js/voip';
import { wamPlugin } from '@zapo-js/wam';
import {
  WaClient,
  WaClientOptions,
  WaClientPluginDefinition,
  WaIncomingMessageEvent,
  WaStore,
} from 'zapo-js';

import { ZapoStoreFactoryCore } from './ZapoStoreFactoryCore';
import { ZapoConfig } from './types';

export class WhatsappSessionZapoCore extends WhatsappSession {
  engine = WAHAEngine.ZAPO;

  protected engineConfig?: ZapoConfig;

  protected storeFactory = new ZapoStoreFactoryCore();
  protected store: WaStore;
  protected client: WaClient;

  private qr: QR = new QR();
  private me: MeInfo | null = null;

  async start() {
    this.status = WAHASessionStatus.STARTING;
    this.buildClient().catch((err) => {
      this.logger.error('Failed to start the client');
      this.logger.error(err, err.stack);
      this.status = WAHASessionStatus.FAILED;
    });
  }

  protected async buildClient() {
    this.store = this.storeFactory.buildStore(this.sessionStore, this.name);
    this.client = new WaClient(this.getClientOptions(), this.logger as any);
    this.listenAuthEvents();
    this.listenConnectionEvents();
    this.subscribeEngineEvents();
    await this.client.connect();
  }

  protected getClientOptions(): WaClientOptions {
    return {
      store: this.store,
      sessionId: this.name,
      plugins: this.getPlugins(),
      media: this.getMediaOptions(),
    };
  }

  /**
   * @zapo-js/native needs no wiring: zapo resolves the fastest crypto backend
   * available at load time (napi, then the bundled wasm, then pure JS).
   */
  protected getPlugins(): WaClientPluginDefinition[] {
    const plugins: WaClientPluginDefinition[] = [];
    if (this.engineConfig?.wam ?? true) {
      plugins.push(wamPlugin());
    }
    if (this.engineConfig?.voip ?? false) {
      plugins.push(voipPlugin());
    }
    return plugins;
  }

  protected getMediaOptions(): WaClientOptions['media'] {
    if (!(this.engineConfig?.media ?? true)) {
      return undefined;
    }
    return { processor: createMediaProcessor() };
  }

  protected listenAuthEvents() {
    this.client.on('auth_qr', ({ qr }) => {
      this.qr.save(qr);
      this.printQR(this.qr);
      this.status = WAHASessionStatus.SCAN_QR_CODE;
    });

    this.client.on('auth_passkey_required', ({ hasSigner }) => {
      // No signer is configured, so the handshake cannot complete headless -
      // the account owner has to authorize the link from their own device.
      this.logger.warn(
        { hasSigner: hasSigner },
        'WhatsApp requires a passkey to link this device',
      );
      this.setStatus(WAHASessionStatus.PASSKEY_REQUIRED, null);
    });

    this.client.on('auth_paired', ({ credentials }) => {
      this.me = this.buildMeInfo(credentials?.meJid);
      this.logger.info('Paired with WhatsApp');
    });
  }

  protected listenConnectionEvents() {
    this.client.on('connection', (event) => {
      if (event.status === 'open') {
        this.me = this.buildMeInfo(this.client.getCredentials()?.meJid);
        this.status = WAHASessionStatus.WORKING;
        return;
      }
      if (event.isLogout) {
        this.logger.warn('The device has been unlinked, re-pairing required');
        this.status = WAHASessionStatus.FAILED;
        return;
      }
      // zapo does not auto-reconnect - connect() has to be called again.
      this.logger.warn({ reason: event.reason }, 'Connection closed');
      this.status = WAHASessionStatus.FAILED;
    });
  }

  protected subscribeEngineEvents() {
    const messages$ = new Observable<WaIncomingMessageEvent>((subscriber) => {
      const listener = (event: WaIncomingMessageEvent) =>
        subscriber.next(event);
      this.client.on('message', listener);
      return () => this.client.off('message', listener);
    });

    const payloads$ = messages$.pipe(
      filter((event) => this.jids.include(event.key?.remoteJid)),
      map((event) => this.toWAMessage(event)),
    );

    this.events2
      .get(WAHAEvents.MESSAGE)
      .switch(payloads$.pipe(filter((message) => !message.fromMe)));
    this.events2.get(WAHAEvents.MESSAGE_ANY).switch(payloads$);
  }

  protected toWAMessage(event: WaIncomingMessageEvent): any {
    const text =
      event.message?.conversation ??
      event.message?.extendedTextMessage?.text ??
      null;
    return {
      id: event.key?.id,
      timestamp: event.timestampSeconds,
      from: event.key?.remoteJid,
      fromMe: event.key?.fromMe,
      participant: event.key?.participant,
      body: text,
      hasMedia: false,
      _data: event.message,
    };
  }

  protected buildMeInfo(meJid?: string | null): MeInfo | null {
    if (!meJid) {
      return null;
    }
    return { id: meJid, pushName: null };
  }

  async stop() {
    await this.client?.disconnect();
    await this.store?.destroy();
    this.client = undefined;
    this.store = undefined;
    this.status = WAHASessionStatus.STOPPED;
    this.stopEvents();
  }

  async unpair() {
    this.unpairing = true;
    await this.client?.logout();
  }

  public getSessionMeInfo(): MeInfo | null {
    if (!this.me) {
      return null;
    }
    return { ...this.me, reachoutTimelock: this.reachoutTimelock.value };
  }

  /**
   * Auth methods
   */
  public getQR(): QR {
    return this.qr;
  }

  @Activity()
  public async requestCode(
    phoneNumber: string,
    method: string,
    params?: any,
  ): Promise<PairingCodeResponse> {
    if (method) {
      const err = `ZAPO engine doesn't support any 'method', remove it and try again`;
      throw new UnprocessableEntityException(err);
    }
    if (this.status !== WAHASessionStatus.SCAN_QR_CODE) {
      const err = `Can request code only in SCAN_QR_CODE status. The current status is ${this.status}`;
      throw new UnprocessableEntityException(err);
    }
    const code = await this.client.auth.requestPairingCode(
      phoneNumber,
      true,
      params?.code,
    );
    this.logger.info({ code: code }, 'Pairing code');
    return { code: code };
  }

  async getScreenshot(): Promise<Buffer> {
    if (this.status === WAHASessionStatus.STARTING) {
      throw new UnprocessableEntityException(
        `The session is starting, please try again after few seconds`,
      );
    }
    if (this.status === WAHASessionStatus.SCAN_QR_CODE) {
      return this.qr.get();
    }
    throw new UnprocessableEntityException(
      `The session is ${this.status}. The screenshot is available only in SCAN_QR_CODE status`,
    );
  }

  /**
   * Messages
   */
  @Activity()
  async sendText(request: MessageTextRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    return this.client.message.send(chatId, {
      type: 'text',
      text: request.text,
    });
  }

  @Activity()
  async startTyping(request: ChatRequest): Promise<void> {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    await this.client.presence.sendChatstate(chatId, { state: 'composing' });
  }

  @Activity()
  async stopTyping(request: ChatRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    await this.client.presence.sendChatstate(chatId, { state: 'paused' });
  }

  /**
   * Not implemented yet - the engine is being built incrementally and these
   * land with media, groups and chats support.
   */
  checkNumberStatus(request: CheckNumberStatusQuery) {
    throw new NotImplementedByEngineError();
  }

  sendLocation(request: MessageLocationRequest) {
    throw new NotImplementedByEngineError();
  }

  forwardMessage(request: MessageForwardRequest): Promise<WAMessage> {
    throw new NotImplementedByEngineError();
  }

  sendImage(request: MessageImageRequest) {
    throw new NotImplementedByEngineError();
  }

  sendFile(request: MessageFileRequest) {
    throw new NotImplementedByEngineError();
  }

  sendVoice(request: MessageVoiceRequest) {
    throw new NotImplementedByEngineError();
  }

  reply(request: MessageReplyRequest) {
    throw new NotImplementedByEngineError();
  }

  sendSeen(chat: SendSeenRequest) {
    throw new NotImplementedByEngineError();
  }

  setReaction(request: MessageReactionRequest) {
    throw new NotImplementedByEngineError();
  }

  readChatMessages(
    chatId: string,
    request: ReadChatMessagesQuery,
  ): Promise<ReadChatMessagesResponse> {
    throw new NotImplementedByEngineError();
  }

  fetchContactProfilePicture(id: string): Promise<string | null> {
    throw new NotImplementedByEngineError();
  }
}
