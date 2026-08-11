import { UnprocessableEntityException } from '@nestjs/common';
import { Activity } from '@waha/core/abc/activity';
import { WhatsappSession } from '@waha/core/abc/session.abc';
import { NotImplementedByEngineError } from '@waha/core/exceptions';
import { QR } from '@waha/core/QR';
import { parseMessageIdSerialized } from '@waha/core/utils/ids';
import { toCusFormat, toJID } from '@waha/core/utils/jids';
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
  ACK_UNKNOWN,
  WAHAEngine,
  WAHAEvents,
  WAHASessionStatus,
  WAMessageAck,
} from '@waha/structures/enums.dto';
import { WAMessage } from '@waha/structures/responses.dto';
import { BinaryFile, RemoteFile } from '@waha/structures/files.dto';
import { WAMimeType } from '@waha/core/media/WAMimeType';
import { PairingCodeResponse } from '@waha/structures/auth.dto';
import { MeInfo } from '@waha/structures/sessions.dto';
import { SECOND } from '@waha/structures/enums.dto';
import { WAMessageAckBody } from '@waha/structures/webhooks.dto';
import { SingleDelayedJobRunner } from '@waha/utils/SingleDelayedJobRunner';
import { merge, Observable, Subject } from 'rxjs';
import { filter, map, mergeMap } from 'rxjs/operators';
import { createMediaProcessor } from '@zapo-js/media-utils';
import { wamPlugin } from '@zapo-js/wam';
import {
  WaClient,
  WaClientOptions,
  WaClientPluginDefinition,
  WaIncomingAddonEvent,
  WaIncomingMessageEvent,
  WaIncomingPresenceEvent,
  WaIncomingProtocolMessageEvent,
  WaIncomingReceiptEvent,
  WaMessagePublishResult,
  WaSendMessageContent,
  WaSendMessageOptions,
  WaStore,
} from 'zapo-js';

import { ZapoStoreFactoryCore } from './ZapoStoreFactoryCore';
import { ZapoConfig } from './types';

const ZAPO_RECEIPT_ACK: Record<string, WAMessageAck> = {
  delivered: WAMessageAck.DEVICE,
  read: WAMessageAck.READ,
  played: WAMessageAck.PLAYED,
};

export class WhatsappSessionZapoCore extends WhatsappSession {
  engine = WAHAEngine.ZAPO;

  protected engineConfig?: ZapoConfig;

  private RESTART_DELAY_SECONDS = 2;

  protected storeFactory = new ZapoStoreFactoryCore();
  protected store: WaStore;
  protected client: WaClient;

  private qr: QR = new QR();
  private me: MeInfo | null = null;

  // Acks the engine issues itself (the server ack of an outgoing message),
  // merged with the ones derived from inbound receipts.
  private sentAcks$ = new Subject<WAMessageAckBody>();

  private restartJob: SingleDelayedJobRunner;
  private shouldRestart: boolean;

  async start() {
    this.status = WAHASessionStatus.STARTING;
    this.shouldRestart = true;
    if (!this.restartJob) {
      this.restartJob = new SingleDelayedJobRunner(
        'restart-engine',
        this.RESTART_DELAY_SECONDS * SECOND,
        this.logger,
      );
    }
    this.buildClient().catch((err) => {
      this.logger.error('Failed to start the client');
      this.logger.error(err, err.stack);
      this.status = WAHASessionStatus.FAILED;
      this.restartClient();
    });
  }

  /**
   * zapo never reconnects on its own - the docs are explicit that connect()
   * has to be called again. Without this the session dies on the first
   * network blip and only comes back with a manual restart.
   */
  private restartClient() {
    if (!this.shouldRestart) {
      this.logger.debug('Should not restart the client, ignoring the request');
      return;
    }
    this.restartJob.schedule(async () => {
      if (!this.shouldRestart) {
        this.logger.warn('Should not restart the client, ignoring the request');
        return;
      }
      this.logger.info('Restarting the client connection...');
      await this.endClient();
      await this.start();
    });
  }

  private async endClient() {
    try {
      await this.client?.disconnect();
    } catch (err) {
      this.logger.warn(`Error while disconnecting the client: ${err}`);
    }
    try {
      await this.store?.destroy();
    } catch (err) {
      this.logger.warn(`Error while destroying the store: ${err}`);
    }
    this.client = undefined;
    this.store = undefined;
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
      plugins.push(this.buildVoipPlugin());
    }
    return plugins;
  }

  /**
   * Loaded on demand: @zapo-js/voip requires @roamhq/wrtc and libmlow-wasm at
   * module load, and neither is installed. Importing it at the top of the file
   * takes the whole application down at boot, not just this engine.
   */
  protected buildVoipPlugin(): WaClientPluginDefinition {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { voipPlugin } = require('@zapo-js/voip');
    return voipPlugin();
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
        // Re-pairing is required, restarting would only spin on a dead device.
        this.logger.warn('The device has been unlinked, re-pairing required');
        this.shouldRestart = false;
        this.status = WAHASessionStatus.FAILED;
        return;
      }
      this.logger.warn({ reason: event.reason }, 'Connection closed');
      this.status = WAHASessionStatus.FAILED;
      this.restartClient();
    });
  }

  /**
   * Bridges a zapo client event into an observable, unsubscribing the
   * listener when the stream is torn down.
   */
  protected fromClientEvent<T>(event: string): Observable<T> {
    return new Observable<T>((subscriber) => {
      const listener = (payload: T) => subscriber.next(payload);
      this.client.on(event as any, listener);
      return () => this.client?.off(event as any, listener);
    });
  }

  protected subscribeEngineEvents() {
    const payloads$ = this.fromClientEvent<WaIncomingMessageEvent>(
      'message',
    ).pipe(
      filter((event) => this.jids.include(event.key?.remoteJid)),
      map((event) => this.toWAMessage(event)),
    );

    this.events2
      .get(WAHAEvents.MESSAGE)
      .switch(payloads$.pipe(filter((message) => !message.fromMe)));
    this.events2.get(WAHAEvents.MESSAGE_ANY).switch(payloads$);

    // delivered / read / played come from inbound receipts, while the server
    // ack (sent) and the failures come from the send call itself.
    const receiptAcks$ = this.fromClientEvent<WaIncomingReceiptEvent>(
      'receipt',
    ).pipe(
      filter((event) => this.jids.include(event.chatJid)),
      mergeMap((event) => this.toMessageAcks(event)),
    );
    this.events2
      .get(WAHAEvents.MESSAGE_ACK)
      .switch(merge(receiptAcks$, this.sentAcks$));

    // Reactions and edits arrive as encrypted addons attached to a parent
    // message, not as messages of their own.
    const addons$ = this.fromClientEvent<WaIncomingAddonEvent>(
      'message_addon',
    ).pipe(filter((event) => this.jids.include(event.key?.remoteJid)));

    this.events2.get(WAHAEvents.MESSAGE_REACTION).switch(
      addons$.pipe(
        filter((event) => event.kind === 'reaction'),
        map((event) => this.toReaction(event)),
      ),
    );

    this.events2.get(WAHAEvents.MESSAGE_EDITED).switch(
      addons$.pipe(
        filter((event) => event.kind === 'message_edit'),
        map((event) => this.toEdited(event)),
      ),
    );

    this.events2.get(WAHAEvents.MESSAGE_REVOKED).switch(
      this.fromClientEvent<WaIncomingProtocolMessageEvent>(
        'message_protocol',
      ).pipe(
        filter((event) => this.jids.include(event.key?.remoteJid)),
        filter((event) => !!event.protocolMessage?.key),
        map((event) => this.toRevoked(event)),
      ),
    );

    this.events2
      .get(WAHAEvents.PRESENCE_UPDATE)
      .switch(
        this.fromClientEvent<WaIncomingPresenceEvent>('presence').pipe(
          map((event) => this.toPresence(event)),
        ),
      );
  }

  protected toReaction(event: WaIncomingAddonEvent): any {
    const decrypted = event.decrypted as any;
    return {
      id: event.key?.id,
      timestamp: event.key?.['timestampSeconds'],
      from: toCusFormat(event.key?.remoteJid),
      fromMe: event.key?.fromMe,
      participant: toCusFormat(event.key?.participant),
      reaction: {
        text: decrypted?.text ?? decrypted?.emoji ?? '',
        messageId: event.targetMessageId,
      },
      _data: event.decrypted,
    };
  }

  protected toEdited(event: WaIncomingAddonEvent): any {
    const decrypted = event.decrypted as any;
    const message = decrypted?.editedMessage ?? decrypted?.message;
    return {
      id: event.key?.id,
      timestamp: event.key?.['timestampSeconds'],
      from: toCusFormat(event.key?.remoteJid),
      fromMe: event.key?.fromMe,
      participant: toCusFormat(event.key?.participant),
      body: this.extractText(message),
      editedMessageId: event.targetMessageId,
      _data: event.decrypted,
    };
  }

  protected toRevoked(event: WaIncomingProtocolMessageEvent): any {
    const target = event.protocolMessage?.key;
    return {
      revokedMessageId: target?.id,
      after: null,
      before: null,
      _data: {
        from: toCusFormat(event.key?.remoteJid),
        participant: toCusFormat(event.key?.participant),
        key: target,
      },
    };
  }

  protected toPresence(event: WaIncomingPresenceEvent): any {
    const chatId = toCusFormat(event.chatJid);
    return {
      id: chatId,
      presences: [
        {
          participant: chatId,
          lastSeen: event.lastSeen ?? null,
          lastKnownPresence: event.type,
        },
      ],
    };
  }

  protected extractText(message: any): string | null {
    return message?.conversation ?? message?.extendedTextMessage?.text ?? null;
  }

  /**
   * A single receipt stanza can acknowledge a batch of message ids, so it
   * fans out into one ack payload per id.
   */
  protected toMessageAcks(event: WaIncomingReceiptEvent): WAMessageAckBody[] {
    const ack = ZAPO_RECEIPT_ACK[event.status];
    if (!ack) {
      // 'inactive' is a presence hint, not an acknowledgement.
      return [];
    }
    // A receipt from our own other device acknowledges someone else's
    // message; anything else acknowledges a message we sent.
    const fromMe = !event.fromSelfDevice;
    const ids = event.messageIds?.length
      ? event.messageIds
      : [event.stanzaId].filter(Boolean);
    return ids.map((id) => this.buildAckBody(id, event.chatJid, ack, fromMe));
  }

  protected buildAckBody(
    id: string,
    chatJid: string,
    ack: WAMessageAck,
    fromMe: boolean,
    error?: number,
  ): WAMessageAckBody {
    const chatId = toCusFormat(chatJid);
    const meId = toCusFormat(this.getSessionMeInfo()?.id);
    const body: WAMessageAckBody = {
      id: id,
      from: fromMe ? meId : chatId,
      to: fromMe ? chatId : meId,
      participant: null,
      fromMe: fromMe,
      ack: ack,
      ackName: WAMessageAck[ack] || ACK_UNKNOWN,
      _data: { chatJid: chatJid, error: error },
    };
    return body;
  }

  /**
   * Publishes the SERVER ack (or ERROR when WhatsApp rejected the publish)
   * for a message this session just sent.
   */
  protected emitSentAck(chatJid: string, result: WaMessagePublishResult) {
    const error = result?.ack?.error;
    const ack = error ? WAMessageAck.ERROR : WAMessageAck.SERVER;
    this.sentAcks$.next(
      this.buildAckBody(result?.id, chatJid, ack, true, error),
    );
  }

  /**
   * Every outgoing message goes through here so the ack stream stays
   * complete no matter which send method was used.
   */
  protected async publish(
    chatJid: string,
    content: WaSendMessageContent,
    options?: WaSendMessageOptions,
  ): Promise<WaMessagePublishResult> {
    const result = await this.client.message.send(chatJid, content, options);
    this.emitSentAck(chatJid, result);
    return result;
  }

  protected toWAMessage(event: WaIncomingMessageEvent): any {
    const text = this.extractText(event.message);
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
    // Order matters: drop the restart intent before closing the socket, or
    // the 'connection' close handler schedules a restart of a stopped session.
    this.shouldRestart = false;
    this.restartJob?.cancel();
    await this.endClient();
    this.status = WAHASessionStatus.STOPPED;
    this.stopEvents();
  }

  async unpair() {
    this.unpairing = true;
    this.shouldRestart = false;
    this.restartJob?.cancel();
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
    return this.publish(chatId, { type: 'text', text: request.text });
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
   * Turns the WAHA file payload (remote url or inline base64) into the bytes
   * zapo's media builder expects.
   */
  protected async fileToMedia(file: BinaryFile | RemoteFile): Promise<Buffer> {
    if ('url' in file && file.url) {
      return this.fetchFile(file.url);
    }
    if ('data' in file && file.data) {
      return Buffer.from(file.data, 'base64');
    }
    throw new UnprocessableEntityException(
      'Either "file.url" or "file.data" must be specified.',
    );
  }

  protected async fetchFile(url: string): Promise<Buffer> {
    const response = await fetch(url);
    if (!response.ok) {
      throw new UnprocessableEntityException(
        `Failed to download the file from '${url}': ${response.status}`,
      );
    }
    return Buffer.from(await response.arrayBuffer());
  }

  /**
   * Builds the send options, resolving 'reply_to' into the quote zapo fills
   * the context info from.
   */
  protected buildSendOptions(request: {
    chatId: string;
    reply_to?: string;
  }): WaSendMessageOptions {
    if (!request.reply_to) {
      return {};
    }
    const key = parseMessageIdSerialized(request.reply_to, true);
    return {
      quote: {
        id: key.id,
        remoteJid: toJID(this.ensureSuffix(request.chatId)),
        participant: key.participant,
      },
    };
  }

  @Activity()
  async sendImage(request: MessageImageRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    const media = await this.fileToMedia(request.file);
    return this.publish(
      chatId,
      {
        type: 'image',
        media: media,
        mimetype: request.file.mimetype,
        caption: request.caption,
      },
      this.buildSendOptions(request),
    );
  }

  @Activity()
  async sendFile(request: MessageFileRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    const media = await this.fileToMedia(request.file);
    return this.publish(
      chatId,
      {
        type: 'document',
        media: media,
        mimetype: request.file.mimetype,
        caption: request.caption,
        fileName: request.file.filename,
      },
      this.buildSendOptions(request),
    );
  }

  @Activity()
  async sendVoice(request: MessageVoiceRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    let media = await this.fileToMedia(request.file);
    let mimetype = request.file.mimetype;
    if (request.convert) {
      media = await this.mediaConverter.voice(media);
      mimetype = WAMimeType.VOICE;
    }
    return this.publish(
      chatId,
      {
        type: 'audio',
        media: media,
        mimetype: mimetype || WAMimeType.VOICE,
        ptt: true,
      },
      this.buildSendOptions(request),
    );
  }

  @Activity()
  async sendLocation(request: MessageLocationRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    return this.publish(
      chatId,
      {
        locationMessage: {
          degreesLatitude: request.latitude,
          degreesLongitude: request.longitude,
          name: request.title || null,
        },
      },
      this.buildSendOptions(request),
    );
  }

  @Activity()
  async reply(request: MessageReplyRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    return this.publish(
      chatId,
      { type: 'text', text: request.text },
      this.buildSendOptions(request),
    );
  }

  @Activity()
  async setReaction(request: MessageReactionRequest) {
    const key = parseMessageIdSerialized(request.messageId);
    const chatId = toJID(this.ensureSuffix(key.remoteJid));
    // An empty emoji revokes the reaction, which is what WAHA sends too.
    await this.client.message.send(chatId, {
      type: 'reaction',
      emoji: request.reaction,
      target: {
        id: key.id,
        remoteJid: chatId,
        fromMe: key.fromMe,
        participant: key.participant,
      },
    });
  }

  @Activity()
  async sendSeen(request: SendSeenRequest) {
    const chatId = toJID(this.ensureSuffix(request.chatId));
    const key = parseMessageIdSerialized(request.messageId, true);
    await this.client.message.sendReceipt(chatId, key.id, {
      type: 'read',
      participant: request.participant,
    });
  }

  @Activity()
  async fetchContactProfilePicture(id: string): Promise<string | null> {
    const jid = toJID(this.ensureSuffix(id));
    const picture = await this.client.profile.getProfilePicture(jid);
    return picture?.url ?? null;
  }

  /**
   * Not implemented yet.
   *
   * forwardMessage and readChatMessages need the message archive wired up
   * (zapo keeps it in the 'messages' store domain), and zapo exposes no
   * usync contact lookup, which is what checkNumberStatus needs.
   */
  checkNumberStatus(request: CheckNumberStatusQuery) {
    throw new NotImplementedByEngineError();
  }

  forwardMessage(request: MessageForwardRequest): Promise<WAMessage> {
    throw new NotImplementedByEngineError();
  }

  readChatMessages(
    chatId: string,
    request: ReadChatMessagesQuery,
  ): Promise<ReadChatMessagesResponse> {
    throw new NotImplementedByEngineError();
  }
}
