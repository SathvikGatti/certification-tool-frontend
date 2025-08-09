/**
 *
 * Copyright (c) 2025 Project CHIP Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable, Subscription, from } from 'rxjs';
import { catchError, filter, take } from "rxjs/operators";
import { WebRTCWebSocketService, WebRTCRequest, WebRTCMessage } from '../web_sockets/webrtc-ws-config';

export const MessageEventTypes = Object.freeze({
  CREATE_PEER_CONNECTION: "CREATE_PEER_CONNECTION",
  CREATE_OFFER: "CREATE_OFFER",
  CREATE_ANSWER: "CREATE_ANSWER",
  SET_REMOTE_OFFER: "SET_REMOTE_OFFER",
  SET_REMOTE_ANSWER: "SET_REMOTE_ANSWER",
  SET_REMOTE_ICE_CANDIDATES: "SET_REMOTE_ICE_CANDIDATES",
  GET_LOCAL_ICE_CANDIDATES: "GET_LOCAL_ICE_CANDIDATES",
  PEER_CONNECTION_STATE: "PEER_CONNECTION_STATE",
  GATHERING_STATE_COMPLETE: "GATHERING_STATE_COMPLETE",
  GET_PEER_CONNECTION_STATE: "GET_PEER_CONNECTION_STATE"
} as const);

export interface RTCIceCandidate {
  candidate: string;
  sdpMLineIndex: number | null;
  sdpMid: string | null;
}

/**
 * WebRTCSession - Represents a single peer connection session
 */
export class WebRTCSession {
  private static localStream: MediaStream | null = null;
  private peerConnection: RTCPeerConnection | null = null;
  private remoteStream: MediaStream | null = null;
  private isInitiator = false;

  private _remoteStream$ = new BehaviorSubject<MediaStream | null>(null);
  private _connectionState$ = new BehaviorSubject<string>('new');
  private _iceGatheringState$ = new BehaviorSubject<string>('new');

  public remoteStream$ = this._remoteStream$.asObservable();
  public connectionState$ = this._connectionState$.asObservable();
  public iceGatheringState$ = this._iceGatheringState$.asObservable();

  constructor(
    public readonly sessionId: string,
    private webrtcWebSocketService: WebRTCWebSocketService,
    private localStream$: Observable<MediaStream | null>
  ) { }

  public initializePeerConnection(request: WebRTCRequest): void {
    const configuration: RTCConfiguration = {
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' }
      ]
    };

    this.peerConnection = new RTCPeerConnection(configuration);

    //TODO: would need the create peer connection message to tell us what this should be
    // use these for now to include two way talk back as well
    this.peerConnection.addTransceiver("audio", { direction: "sendrecv"})
    this.peerConnection.addTransceiver("video", { direction: "recvonly"})

    this.localStream$
    .pipe(
      filter((stream): stream is MediaStream => !!stream),
      take(1)
    )
    .subscribe(stream => {
      stream.getTracks().forEach(track => {
        this.peerConnection!.addTrack(track);
      });
    });

    // Set up event listeners and these will be reported as we receive them
    this.peerConnection.onicecandidate = (event) => {
      if (event.candidate && event.candidate?.candidate != "") {
        this.sendIceCandidate(event.candidate, request);
      }
    };

    this.peerConnection.ontrack = (event) => {
      this.remoteStream?.addTrack(event.track);
      this._remoteStream$.next(this.remoteStream);
      console.log(`Session ${this.sessionId}: Remote stream received`);
    };

    this.peerConnection.onconnectionstatechange = () => {
      const state = this.peerConnection?.connectionState || 'unknown';
      console.debug(`Session ${this.sessionId}: Connection state changed to ${state}`);
      this._connectionState$.next(state);
      const response = request.createResponse(state, null, MessageEventTypes.PEER_CONNECTION_STATE);
      this.webrtcWebSocketService.send(response);
    };

    this.peerConnection.onicegatheringstatechange = () => {
      const state = this.peerConnection?.iceGatheringState || 'unknown';
      console.log(`Session ${this.sessionId}: ICE connection state changed to ${state}`);
      if (state == "complete"){
        this._iceGatheringState$.next(state);
        const response = request.createResponse(state, null, MessageEventTypes.GATHERING_STATE_COMPLETE)
        this.webrtcWebSocketService.send(response)
      }
    };

    this.webrtcWebSocketService.send(request.createResponse());
  }

  public async createOffer(request: WebRTCRequest): Promise<void> {
    try {
      this.isInitiator = true;
      const offer = await this.peerConnection!.createOffer();
      await this.peerConnection!.setLocalDescription(offer);
      this.webrtcWebSocketService.send(request.createResponse(offer))
    } catch (error) {
      const err = `Session ${this.sessionId}: Error creating offer: ${error}`;
      this.sendErrorResponse(err, request);
    }
  }

  public async createAnswer(request: WebRTCRequest): Promise<void> {
    try {
      this.isInitiator = false;
      const answer = await this.peerConnection!.createAnswer();
      await this.peerConnection!.setLocalDescription(answer);
      const response = request.createResponse(answer);
      this.webrtcWebSocketService.send(response);
    } catch (error) {
      const err = `Session ${this.sessionId}: Error creating answer: ${error}`
      this.sendErrorResponse(err, request);
    }
  }

  public async setRemoteOffer(request: WebRTCRequest): Promise<void> {
    try {
      const message = request.message;
      const remoteDesc = new RTCSessionDescription({
        type: "offer",
        sdp: message.data
      });
      await this.peerConnection!.setRemoteDescription(remoteDesc);
      const response = request.createResponse();
      this.webrtcWebSocketService.send(response);
    } catch (error) {
      const err = `Session ${this.sessionId}: Error setting remote offer: ${error}`
      this.sendErrorResponse(err, request);
    }
  }

  public async setRemoteAnswer(request: WebRTCRequest): Promise<void> {
    try {
      const message = request.message
      const remoteDesc = new RTCSessionDescription({
        type: "answer",
        sdp: message.data
      });
      
      await this.peerConnection!.setRemoteDescription(remoteDesc);
      const response = request.createResponse();
      this.webrtcWebSocketService.send(response);
    } catch (error) {
      const err = `Session ${this.sessionId}: Error setting remote answer: ${error}`
      this.sendErrorResponse(err, request);
    }
  }

  public async setRemoteIceCandidates(request: WebRTCRequest): Promise<void> {
    try {
      const message = request.message
      for (const candidateData of message.data) {
        const candidate = new RTCIceCandidate({candidate: candidateData, sdpMLineIndex: null, sdpMid: null});
        await this.peerConnection!.addIceCandidate(candidate);
      }
      this.webrtcWebSocketService.send(request.createResponse());
    } catch (error) {
      const err = `Session ${this.sessionId}: Error setting remote ICE candidates: ${error}`;
      this.sendErrorResponse(err, request);
    }
  }

  public reportPeerConnectionState(request: WebRTCRequest): void {
    const state = this.connectionState$;
    const response = request.createResponse(state);
    this.webrtcWebSocketService.send(response);
  }

  private sendIceCandidate(candidate: RTCIceCandidate, request: WebRTCRequest): void {
    const response = request.createResponse(candidate.candidate, null, MessageEventTypes.GET_LOCAL_ICE_CANDIDATES);
    this.webrtcWebSocketService.send(response);
  }

  private sendErrorResponse(errorMessage: string, request: WebRTCRequest): void {
    console.error(errorMessage)
    const errorResponse = request.createResponse(null, errorMessage);
    this.webrtcWebSocketService.send(errorResponse);
  }

  public closeConnection(): void {
    if (this.peerConnection) {
      this.peerConnection.close();
      this.peerConnection = null;
    }
    this.remoteStream = null;
    this._remoteStream$.next(null);
    this._connectionState$.next('closed');
    this._iceGatheringState$.next('closed');
    console.log(`Session ${this.sessionId}: Connection closed`);
  }

}

/**
 * WebRTCService - Manages WebRTC sessions
 */
@Injectable({ providedIn: 'root' })
export class WebRTCService {
  private sessionsMap = new Map<string, WebRTCSession>();
  private webrtcMessageSubscription: Subscription | null = null;
  private _sessions$ = new BehaviorSubject<WebRTCSession[]>([]);
  private _localStream$ = new BehaviorSubject<MediaStream | null>(null);

  public sessions$ = this._sessions$.asObservable();
  public localStream$ = this._localStream$.asObservable();

  constructor(private webrtcWebSocketService: WebRTCWebSocketService) {
    this.webrtcWebSocketService.connect();
    this.setupWebSocketSubscription();
  }

  private setupWebSocketSubscription(): void {
    this.webrtcMessageSubscription = this.webrtcWebSocketService.messages$.subscribe(
      (request: WebRTCRequest) => {
        try {
          this.handleWebRTCMessage(request);
        } catch (error) {
          console.error('Error processing WebRTC message:', error);
        }
      }
    );
  }

  public handleWebRTCMessage(request: WebRTCRequest): void {
    const message = request.message;

    if (!message.sessionId) {
      const err = `WebRTC message missing sessionId: ${message.sessionId}`
      console.error(err);
      this.webrtcWebSocketService.send(err);
      return;
    }

    let session = this.getOrCreateSessionInstance(message.sessionId, request);

    // Route message to session based on type
    switch (message.type) {
      case MessageEventTypes.CREATE_PEER_CONNECTION:
        session.initializePeerConnection(request);
        break;
      case MessageEventTypes.CREATE_OFFER:
        session.createOffer(request);
        break;
      case MessageEventTypes.CREATE_ANSWER:
        session.createAnswer(request);
        break;
      case MessageEventTypes.SET_REMOTE_OFFER:
        session.setRemoteOffer(request);
        break;
      case MessageEventTypes.SET_REMOTE_ANSWER:
        session.setRemoteAnswer(request);
        break;
      case MessageEventTypes.SET_REMOTE_ICE_CANDIDATES:
        session.setRemoteIceCandidates(request);
        break;
      case MessageEventTypes.GET_PEER_CONNECTION_STATE:
        session.reportPeerConnectionState(request);
        break;
      default:
        const err = `Unknown WebRTC message type: ${message.type} for session: ${message.sessionId}`
        console.error(err);
        this.webrtcWebSocketService.send(err)
    }
  }

  private getOrCreateSessionInstance(sessionId: string, request: WebRTCRequest): WebRTCSession {
    if (this.sessionsMap.has(sessionId)) {
      return this.sessionsMap.get(sessionId)!;
    }

    if (this._localStream$ == null) {
      from(navigator.mediaDevices.getUserMedia({ audio: true, video: false }))
        .pipe(
          catchError(err => {
            const errmsg = `Failed to get local media: ${err}`
            const errResponse = request.createResponse(null, errmsg);
            this.webrtcWebSocketService.send(errResponse);
            throw err;
          })
        )
        .subscribe(stream => this._localStream$.next(stream));
    }

    const session = new WebRTCSession(sessionId, this.webrtcWebSocketService, this.localStream$);
    this.sessionsMap.set(sessionId, session);
    this.updateActiveSessions();

    console.log(`Created new WebRTC session instance: ${sessionId}`);
    return session;
  }

  public closeSession(sessionId: string): void {
    const session = this.sessionsMap.get(sessionId);
    if (session) {
      session.closeConnection();
      this.sessionsMap.delete(sessionId);
      this.updateActiveSessions();
      console.log(`Closed WebRTC session: ${sessionId}`);
    }
  }

  private updateActiveSessions(): void {
    this._sessions$.next(Array.from(this.sessionsMap.values()));
  }

  public closeAllSessions(): void {
    for (const [sessionId, session] of this.sessionsMap) {
      session.closeConnection();
    }
    this.sessionsMap.clear();
    this.updateActiveSessions();
    console.log('Closed all WebRTC sessionsMap');
  }

  public cleanup(): void {
    this.closeAllSessions();
    if (this.webrtcMessageSubscription) {
      this.webrtcMessageSubscription.unsubscribe();
      this.webrtcMessageSubscription = null;
    }
  }
}
