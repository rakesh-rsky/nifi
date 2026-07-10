/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, ElementRef, inject, OnDestroy, OnInit, Renderer2, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { HttpClient } from '@angular/common/http';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../../state';
import { setCopilotChatOpen } from '../../../state/flow/flow.actions';
import { selectCurrentProcessGroupId } from '../../../state/flow/flow.selectors';
import { EMPTY, interval, Observable, of, Subscription } from 'rxjs';
import { catchError, map, switchMap, take } from 'rxjs/operators';

export type AuthState = 'checking' | 'unauthenticated' | 'device_flow' | 'aws_form' | 'aws_device_flow' | 'aws_role_select' | 'authenticated';
export type Provider = 'github' | 'aws';

export interface ChatMessage {
    role: 'user' | 'assistant';
    content: string;
    timestamp: Date;
    processors?: CreatedProcessor[];
    tokensUsed?: { input: number; output: number; total: number };
    error?: boolean;
}

export interface CreatedProcessor {
    name: string;
    type: string;
    id: string;      // real NiFi UUID
    spec_id: string; // LLM spec ID (e.g. "proc1") — used for append tracking
}

export interface ExistingProcessor {
    spec_id: string;
    nifi_id: string;
    name: string;
    type: string;
}

export interface AuthStatusResponse {
    authenticated: boolean;
    login: string;
    device_flow_active: boolean;
    user_code: string;
    verification_uri: string;
}

export interface AWSAuthStatusResponse {
    authenticated: boolean;
    device_flow_active: boolean;
    user_code: string;
    verification_uri: string;
    role_selection_needed: boolean;
    available_roles: { account_id: string; account_name: string; role_name: string }[];
    bedrock_region: string;
    account_id: string;
    role_name: string;
}

export interface DeviceFlowResponse {
    user_code: string;
    verification_uri: string;
    expires_in: number;
}

export interface ModelOption {
    id: string;
    label: string;
}

export interface ChatRequest {
    message: string;
    process_group_id: string;
    history: { role: string; content: string }[];
    existing_processors: ExistingProcessor[];
    provider: string;
    model: string;
}

export interface ChatResponse {
    reply: string;
    processors_created: CreatedProcessor[];
    connections_created: number;
    tokens_used?: { input: number; output: number; total: number };
}

@Component({
    selector: 'copilot-chat',
    standalone: true,
    templateUrl: './copilot-chat.component.html',
    styleUrls: ['./copilot-chat.component.scss'],
    imports: [CommonModule, FormsModule, MatButtonModule, MatIconModule, MatProgressSpinnerModule, MatTooltipModule]
})
export class CopilotChatComponent implements OnInit, OnDestroy {
    private store = inject<Store<NiFiState>>(Store);
    private http = inject(HttpClient);
    private renderer = inject(Renderer2);

    @ViewChild('messagesEnd') private messagesEnd!: ElementRef;

    readonly backendUrl = 'http://localhost:8080/nifi-api/copilot';
    // readonly backendUrl = (window as any).__COPILOT_BACKEND_URL__ ?? '/nifi-api/copilot';

    // ── Panel resize ────────────────────────────────────────────────────────
    private readonly MIN_PANEL_WIDTH = 280;
    private readonly MAX_PANEL_WIDTH = 900;
    private readonly PANEL_WIDTH_KEY = 'copilot-panel-width';
    panelWidth: number;

    private _resizeStartX = 0;
    private _resizeStartWidth = 0;
    private _removeResizeMove?: () => void;
    private _removeResizeUp?: () => void;

    // ── Session persistence ─────────────────────────────────────────────────
    // Sessions are stored in the nifi-copilot backend (SQLite) keyed by process group ID.

    // ── Auth state ──────────────────────────────────────────────────────────
    authState: AuthState = 'checking';
    provider: Provider = 'github';

    // GitHub
    githubLogin = '';
    githubUserCode = '';
    githubVerificationUri = 'https://github.com/login/device';
    githubDeviceCountdown = 0;
    githubUrlCopied = false;

    // AWS Bedrock
    awsSsoStartUrl = '';
    awsSsoRegion = 'us-east-1';
    awsBedrockRegion = '';
    awsConnecting = false;
    awsConnectError = '';
    awsDeviceCode = '';
    awsVerificationUri = '';
    awsDeviceCountdown = 0;
    awsUrlCopied = false;
    awsAvailableRoles: { account_id: string; account_name: string; role_name: string }[] = [];
    awsBadge = ''; // e.g. "us-east-1 / AdministratorAccess"

    private authPollSub: Subscription | null = null;
    private countdownInterval: any = null;

    // ── Model selection ─────────────────────────────────────────────────────
    readonly githubModels: ModelOption[] = [
        { id: 'gpt-4o', label: 'GPT-4o' },
        { id: 'gpt-4o-mini', label: 'GPT-4o mini' },
        { id: 'o1-mini', label: 'o1-mini' },
        { id: 'Meta-Llama-3.1-70B-Instruct', label: 'Llama 3.1 70B' },
        { id: 'Meta-Llama-3.1-8B-Instruct', label: 'Llama 3.1 8B' }
    ];

    readonly bedrockModels: ModelOption[] = [
        { id: 'us.anthropic.claude-sonnet-4-6', label: 'Claude Sonnet 4.6' },
        { id: 'us.anthropic.claude-sonnet-4-5-20250929-v1:0', label: 'Claude Sonnet 4.5' },
        { id: 'us.anthropic.claude-3-7-sonnet-20250219-v1:0', label: 'Claude 3.7 Sonnet' },
        { id: 'us.anthropic.claude-3-5-sonnet-20241022-v2:0', label: 'Claude 3.5 Sonnet' },
        { id: 'us.anthropic.claude-3-5-haiku-20241022-v1:0', label: 'Claude 3.5 Haiku' },
        { id: 'anthropic.claude-3-haiku-20240307-v1:0', label: 'Claude 3 Haiku' },
        { id: 'amazon.nova-pro-v1:0', label: 'Amazon Nova Pro' },
        { id: 'amazon.nova-lite-v1:0', label: 'Amazon Nova Lite' }
    ];

    selectedModel = this.githubModels[0].id;

    get availableModels(): ModelOption[] {
        return this.provider === 'aws' ? this.bedrockModels : this.githubModels;
    }

    // ── Chat state ──────────────────────────────────────────────────────────
    private readonly welcomeContent =
        'Hi! I\'m AI Copilot for NiFi.\n\nDescribe a data flow and I\'ll build it on the canvas.\n\nExample: "Create a flow that reads from Kafka and writes to S3"';

    messages: ChatMessage[] = [{ role: 'assistant', content: this.welcomeContent, timestamp: new Date() }];
    inputText = '';
    isLoading = false;
    currentProcessGroupId = '';
    codeCopied = false;

    private sessionProcessors: ExistingProcessor[] = [];

    constructor() {
        const saved = localStorage.getItem(this.PANEL_WIDTH_KEY);
        this.panelWidth = saved ? Math.max(this.MIN_PANEL_WIDTH, Math.min(this.MAX_PANEL_WIDTH, +saved)) : 360;
        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(take(1))
            .subscribe((id) => (this.currentProcessGroupId = id));
    }

    ngOnInit(): void {
        this.checkGitHubAuthStatus();
    }

    ngOnDestroy(): void {
        this.stopAuthPolling();
        if (this.countdownInterval) clearInterval(this.countdownInterval);
        this._removeResizeMove?.();
        this._removeResizeUp?.();
    }

    // ── Panel resize ─────────────────────────────────────────────────────────

    onResizeStart(event: MouseEvent): void {
        event.preventDefault();
        this._resizeStartX = event.clientX;
        this._resizeStartWidth = this.panelWidth;

        this._removeResizeMove = this.renderer.listen('document', 'mousemove', (e: MouseEvent) => {
            // Panel is on the right — dragging left widens, dragging right narrows
            const delta = this._resizeStartX - e.clientX;
            this.panelWidth = Math.max(
                this.MIN_PANEL_WIDTH,
                Math.min(this.MAX_PANEL_WIDTH, this._resizeStartWidth + delta)
            );
        });

        this._removeResizeUp = this.renderer.listen('document', 'mouseup', () => {
            localStorage.setItem(this.PANEL_WIDTH_KEY, String(this.panelWidth));
            this._removeResizeMove?.();
            this._removeResizeUp?.();
        });
    }

    // ── Session persistence ───────────────────────────────────────────────────

    /** Fetch session from backend. Restores state and returns true if found. */
    private loadSession(): Observable<boolean> {
        const url = `${this.backendUrl}/api/session/${this.currentProcessGroupId}`;
        return this.http.get<any>(url).pipe(
            map((saved) => {
                const msgs: ChatMessage[] = (saved.messages ?? []).map((m: any) => ({
                    ...m,
                    timestamp: new Date(m.timestamp)
                }));
                if (msgs.length === 0) return false;
                this.messages = msgs;
                this.sessionProcessors = saved.sessionProcessors ?? [];
                if (saved.selectedModel) this.selectedModel = saved.selectedModel;
                return true;
            }),
            catchError(() => of(false))
        );
    }

    /** Persist current session to backend (fire-and-forget). */
    private saveSession(): void {
        const url = `${this.backendUrl}/api/session/${this.currentProcessGroupId}`;
        this.http
            .put(url, {
                messages: this.messages.map((m) => ({ ...m, timestamp: m.timestamp.toISOString() })),
                sessionProcessors: this.sessionProcessors,
                selectedModel: this.selectedModel
            })
            .subscribe({ error: () => {} });
    }

    checkGitHubAuthStatus(): void {
        this.authState = 'checking';
        // Check GitHub first, then AWS
        this.http.get<AuthStatusResponse>(`${this.backendUrl}/api/auth/status`).subscribe({
            next: (res) => {
                if (res.authenticated) {
                    this.authState = 'authenticated';
                    this.provider = 'github';
                    this.githubLogin = res.login;
                    this.loadSession().subscribe((loaded) => {
                        if (!loaded) this.selectedModel = this.githubModels[0].id;
                    });
                } else if (res.device_flow_active) {
                    this.authState = 'device_flow';
                    this.provider = 'github';
                    this.githubUserCode = res.user_code;
                    this.githubVerificationUri = res.verification_uri;
                    this.startGitHubAuthPolling();
                } else {
                    this.checkAWSAuthStatus();
                }
            },
            error: () => this.checkAWSAuthStatus()
        });
    }

    private checkAWSAuthStatus(): void {
        this.http.get<AWSAuthStatusResponse>(`${this.backendUrl}/api/auth/aws/status`).subscribe({
            next: (res) => {
                if (res.authenticated) {
                    this.authState = 'authenticated';
                    this.provider = 'aws';
                    this.awsBadge = `${res.bedrock_region} / ${res.role_name}`;
                    this.loadSession().subscribe((loaded) => {
                        if (!loaded) this.selectedModel = this.bedrockModels[0].id;
                    });
                } else if (res.device_flow_active) {
                    this.authState = 'aws_device_flow';
                    this.provider = 'aws';
                    this.awsDeviceCode = res.user_code;
                    this.awsVerificationUri = res.verification_uri;
                    this.startAWSAuthPolling();
                } else if (res.role_selection_needed) {
                    this.authState = 'aws_role_select';
                    this.provider = 'aws';
                    this.awsAvailableRoles = res.available_roles;
                } else {
                    this.authState = 'unauthenticated';
                }
            },
            error: () => {
                this.authState = 'unauthenticated';
            }
        });
    }

    // ── GitHub sign-in ───────────────────────────────────────────────────────

    signIn(): void {
        this.provider = 'github';
        this.authState = 'checking';
        console.log('🔄 Initiating GitHub device flow...');
        this.http.post<DeviceFlowResponse>(`${this.backendUrl}/api/auth/start`, {}).subscribe({
            next: (res) => {
                console.log('📝 Received device flow response:', {
                    user_code: res.user_code,
                    verification_uri: res.verification_uri
                });
                this.githubUserCode = res.user_code;
                this.githubVerificationUri = res.verification_uri;
                this.githubDeviceCountdown = res.expires_in;
                this.authState = 'device_flow';
                this.startGitHubAuthPolling();
                this.startGitHubCountdown();

                navigator.clipboard
                    .writeText(res.user_code)
                    .then(() => {
                        this.codeCopied = true;
                        setTimeout(() => (this.codeCopied = false), 4000);
                    })
                    .catch(() => {});
            },
            error: (err) => {
                console.error('❌ Failed to start GitHub Device Flow:', err);
                this.authState = 'unauthenticated';
            }
        });
    }

    private startGitHubAuthPolling(): void {
        this.stopAuthPolling();
        console.log('🔵 Starting GitHub auth polling (2s interval)...');
        this.authPollSub = interval(2000)
            .pipe(
                switchMap(() =>
                    this.http.get<AuthStatusResponse>(`${this.backendUrl}/api/auth/status`).pipe(
                        catchError((err) => {
                            console.warn('⚠️ GitHub auth status poll error (will retry):', err);
                            return EMPTY;
                        })
                    )
                )
            )
            .subscribe({
                next: (res) => {
                    console.log('📡 GitHub auth status:', {
                        authenticated: res.authenticated,
                        device_flow_active: res.device_flow_active,
                        login: res.login
                    });
                    if (res.authenticated) {
                        console.log('✅ GitHub authentication successful! Login:', res.login);
                        this.onAuthenticated('github', res.login);
                        this.stopAuthPolling();
                        if (this.countdownInterval) clearInterval(this.countdownInterval);
                    }
                }
            });
    }

    // ── AWS sign-in ──────────────────────────────────────────────────────────

    showAWSForm(): void {
        this.awsConnectError = '';
        this.authState = 'aws_form';
    }

    connectWithAWS(): void {
        if (!this.awsSsoStartUrl.trim()) {
            this.awsConnectError = 'SSO start URL is required.';
            return;
        }
        this.awsConnecting = true;
        this.awsConnectError = '';

        this.http
            .post<DeviceFlowResponse>(`${this.backendUrl}/api/auth/aws/start`, {
                sso_start_url: this.awsSsoStartUrl.trim(),
                sso_region: this.awsSsoRegion.trim() || 'us-east-1',
                bedrock_region: this.awsBedrockRegion.trim()
            })
            .subscribe({
                next: (res) => {
                    this.awsConnecting = false;
                    this.awsDeviceCode = res.user_code;
                    this.awsVerificationUri = res.verification_uri;
                    this.awsDeviceCountdown = res.expires_in;
                    this.authState = 'aws_device_flow';
                    this.startAWSAuthPolling();
                    this.startAWSCountdown();

                    navigator.clipboard
                        .writeText(res.user_code)
                        .then(() => {
                            this.codeCopied = true;
                            setTimeout(() => (this.codeCopied = false), 4000);
                        })
                        .catch(() => {});
                },
                error: (err) => {
                    this.awsConnecting = false;
                    this.awsConnectError =
                        err?.error?.detail ?? 'Failed to start AWS SSO sign-in. Check your SSO URL and region.';
                }
            });
    }

    private startAWSAuthPolling(): void {
        this.stopAuthPolling();
        console.log('🔵 Starting AWS auth polling (2s interval)...');
        this.authPollSub = interval(2000)
            .pipe(
                switchMap(() =>
                    this.http.get<AWSAuthStatusResponse>(`${this.backendUrl}/api/auth/aws/status`).pipe(
                        catchError((err) => {
                            console.warn('⚠️ AWS auth status poll error (will retry):', err);
                            return EMPTY;
                        })
                    )
                )
            )
            .subscribe({
                next: (res) => {
                    console.log('📡 AWS auth status:', {
                        authenticated: res.authenticated,
                        device_flow_active: res.device_flow_active,
                        role_selection: res.role_selection_needed
                    });
                    if (res.authenticated) {
                        console.log('✅ AWS authentication successful!');
                        this.onAuthenticated('aws', `${res.bedrock_region} / ${res.role_name}`);
                        this.stopAuthPolling();
                        if (this.countdownInterval) clearInterval(this.countdownInterval);
                    } else if (res.role_selection_needed) {
                        console.log('⚙️ AWS role selection needed, showing role selector');
                        this.authState = 'aws_role_select';
                        this.awsAvailableRoles = res.available_roles;
                        this.stopAuthPolling();
                        if (this.countdownInterval) clearInterval(this.countdownInterval);
                    }
                }
            });
    }

    selectAWSRole(accountId: string, roleName: string): void {
        this.authState = 'checking';
        this.http
            .post(`${this.backendUrl}/api/auth/aws/select-role`, {
                account_id: accountId,
                role_name: roleName
            })
            .subscribe({
                next: () => {
                    this.http.get<AWSAuthStatusResponse>(`${this.backendUrl}/api/auth/aws/status`).subscribe({
                        next: (res) => {
                            this.onAuthenticated('aws', `${res.bedrock_region} / ${res.role_name}`);
                        },
                        error: () => {
                            this.authState = 'unauthenticated';
                        }
                    });
                },
                error: (err) => {
                    this.awsConnectError = err?.error?.detail ?? 'Failed to select role.';
                    this.authState = 'aws_role_select';
                }
            });
    }

    private startAWSCountdown(): void {
        if (this.countdownInterval) clearInterval(this.countdownInterval);
        this.countdownInterval = setInterval(() => {
            if (this.awsDeviceCountdown > 0) {
                this.awsDeviceCountdown--;
            } else {
                clearInterval(this.countdownInterval);
                if (this.authState === 'aws_device_flow') {
                    this.authState = 'unauthenticated';
                    this.stopAuthPolling();
                }
            }
        }, 1000);
    }

    openAWSBrowser(): void {
        window.open(this.awsVerificationUri, '_blank');
    }

    // ── Shared auth helpers ──────────────────────────────────────────────────

    /** Called every time a sign-in completes. Centralises provider, model, and chat reset. */
    private onAuthenticated(provider: Provider, displayInfo: string = ''): void {
        this.authState = 'authenticated';
        this.provider = provider;
        if (provider === 'github') {
            this.githubLogin = displayInfo;
        } else {
            this.awsBadge = displayInfo;
        }
        this.loadSession().subscribe((loaded) => {
            if (!loaded) {
                this.selectedModel = provider === 'github' ? this.githubModels[0].id : this.bedrockModels[0].id;
                this.messages = [{ role: 'assistant', content: this.welcomeContent, timestamp: new Date() }];
                this.sessionProcessors = [];
            }
        });
    }

    stopAuthPolling(): void {
        if (this.authPollSub) {
            this.authPollSub.unsubscribe();
            this.authPollSub = null;
        }
    }

    private startGitHubCountdown(): void {
        if (this.countdownInterval) clearInterval(this.countdownInterval);
        this.countdownInterval = setInterval(() => {
            if (this.githubDeviceCountdown > 0) {
                this.githubDeviceCountdown--;
            } else {
                clearInterval(this.countdownInterval);
                if (this.authState === 'device_flow') {
                    this.authState = 'unauthenticated';
                    this.stopAuthPolling();
                }
            }
        }, 1000);
    }

    copyCode(): void {
        const code = this.authState === 'aws_device_flow' ? this.awsDeviceCode : this.githubUserCode;
        navigator.clipboard.writeText(code).then(() => {
            this.codeCopied = true;
            setTimeout(() => (this.codeCopied = false), 2000);
        });
    }

    copyUrl(provider: Provider): void {
        const url = provider === 'aws' ? this.awsVerificationUri : this.githubVerificationUri;
        navigator.clipboard
            .writeText(url)
            .then(() => {
                if (provider === 'aws') {
                    this.awsUrlCopied = true;
                    setTimeout(() => (this.awsUrlCopied = false), 2000);
                } else {
                    this.githubUrlCopied = true;
                    setTimeout(() => (this.githubUrlCopied = false), 2000);
                }
            })
            .catch(() => {});
    }

    openGitHub(): void {
        window.open(this.githubVerificationUri, '_blank');
    }

    signOut(): void {
        const url =
            this.provider === 'aws' ? `${this.backendUrl}/api/auth/aws/logout` : `${this.backendUrl}/api/auth/logout`;

        this.http.post(url, {}).subscribe(() => {
            this.authState = 'unauthenticated';
            this.githubLogin = '';
            this.awsBadge = '';
            this.sessionProcessors = [];
            this.messages = [
                {
                    role: 'assistant',
                    content: 'Signed out. Choose a provider below to sign back in.',
                    timestamp: new Date()
                }
            ];
        });
    }

    // ── Chat methods ─────────────────────────────────────────────────────────

    close(): void {
        this.store.dispatch(setCopilotChatOpen({ copilotChatOpen: false }));
    }

    clearChat(): void {
        this.messages = [
            {
                role: 'assistant',
                content: 'Chat cleared. Describe a new flow to build!',
                timestamp: new Date()
            }
        ];
        this.sessionProcessors = [];
        this.http.delete(`${this.backendUrl}/api/session/${this.currentProcessGroupId}`).subscribe({ error: () => {} });
    }

    sendMessage(): void {
        const text = this.inputText.trim();
        if (!text || this.isLoading) return;

        this.messages.push({ role: 'user', content: text, timestamp: new Date() });
        this.inputText = '';
        this.isLoading = true;
        this.scrollToBottom();

        const history = this.messages.slice(0, -1).map((m) => ({ role: m.role, content: m.content }));

        const payload: ChatRequest = {
            message: text,
            process_group_id: this.currentProcessGroupId,
            history,
            existing_processors: [...this.sessionProcessors],
            provider: this.provider,
            model: this.selectedModel
        };

        this.http.post<ChatResponse>(`${this.backendUrl}/api/chat`, payload).subscribe({
            next: (res) => {
                for (const p of res.processors_created) {
                    this.sessionProcessors.push({
                        spec_id: p.spec_id,
                        nifi_id: p.id,
                        name: p.name,
                        type: p.type
                    });
                }
                this.messages.push({
                    role: 'assistant',
                    content: res.reply,
                    timestamp: new Date(),
                    processors: res.processors_created,
                    tokensUsed: res.tokens_used
                });
                this.isLoading = false;
                this.saveSession();
                this.scrollToBottom();
            },
            error: (err) => {
                const status = err?.status;
                if (status === 401) {
                    this.authState = 'unauthenticated';
                    this.githubLogin = '';
                    this.awsBadge = '';
                }
                const detail = err?.error?.detail ?? err?.message ?? 'Unknown error';
                this.messages.push({
                    role: 'assistant',
                    content: `⚠️ ${detail}`,
                    timestamp: new Date(),
                    error: true
                });
                this.isLoading = false;
                this.saveSession();
                this.scrollToBottom();
            }
        });
    }

    onKeyDown(event: KeyboardEvent): void {
        if (event.key === 'Enter' && !event.shiftKey) {
            event.preventDefault();
            this.sendMessage();
        }
    }

    private scrollToBottom(): void {
        setTimeout(() => {
            this.messagesEnd?.nativeElement?.scrollIntoView({ behavior: 'smooth' });
        }, 50);
    }

    trackByIndex(index: number): number {
        return index;
    }

    formatCountdown(seconds: number): string {
        const m = Math.floor(seconds / 60);
        const s = seconds % 60;
        return `${m}:${s.toString().padStart(2, '0')}`;
    }
}
