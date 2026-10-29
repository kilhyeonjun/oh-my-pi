/**
 * MCP HTTP transport (Streamable HTTP).
 *
 * Implements JSON-RPC 2.0 over HTTP POST with optional SSE streaming.
 * The negotiated protocol revision is carried in the `MCP-Protocol-Version`
 * header on every request (see `MCP_PROTOCOL_VERSION`).
 */
import * as AIError from "@oh-my-pi/pi-ai/error";
import { isRecord, logger, postmortem, readSseEvents, readSseJson, untilAborted } from "@oh-my-pi/pi-utils";
import type {
	JsonRpcError,
	JsonRpcMessage,
	JsonRpcRequest,
	MCPHttpServerConfig,
	MCPRequestOptions,
	MCPSseServerConfig,
	MCPTransport,
	MCPTransportOptions,
} from "../../mcp/types";
import { toJsonRpcError } from "../../mcp/types";
import {
	createMCPJsonRpcError,
	isMCPAdmissionError,
	MCPAdmissionError,
	type MCPFailureStage,
	MCPTransportError,
	mcpTraceIdFromHeaders,
	normalizeMCPTransportError,
} from "../errors";
import { RequestIdAllocator } from "../request-id";
import {
	bindMCPTransport,
	createMCPControlCustody,
	createMCPRequestCustody,
	isGuardedMCPTransport,
	type MCPSendCustody,
} from "../invocation";
import {
	createMCPTimeout,
	getNeverAbortSignal,
	isMCPTimeoutEnabled,
	type MCPTimeoutOperation,
	resolveMCPTimeoutMs,
} from "../timeout";
import { type MCPFetchInit, mcpFetch, withoutHeader } from "./header-policy";

const HTTP_SSE_CONNECT_TIMEOUT_MS = 1_000;
const DEFAULT_SSE_RETRY_MS = 3_000;

// Native OAuth discovery parses challenge hints from an Error message. Keep that
// input out of the public diagnostic error rather than changing its decoder.
let guardedAuthChallenges: WeakMap<Error, Error> | undefined;
/** Internal native auth input only; never log or expose the returned challenge. */
export function getMCPHttpAuthError(error: Error): Error {
	return guardedAuthChallenges?.get(error) ?? error;
}

interface GuardedRequestOperation {
	operation: MCPTimeoutOperation;
	timeout: number;
	releaseCaller?: () => void;
}

interface SSEResumeState {
	lastEventId: string | null;
	retryMs: number;
}

/**
 * Failure resuming an accepted request's logical SSE stream. Carries a
 * never-replay contract: by resume time the server has accepted (and possibly
 * executed) the originating POST, so auth-retry paths must not re-send it.
 */
class SSEResumeError extends Error {}

/** Wait for the server-provided SSE retry interval while remaining abortable. */
async function waitForSSERetry(ms: number, signal: AbortSignal): Promise<void> {
	if (signal.aborted) throw signal.reason;
	const { promise, resolve, reject } = Promise.withResolvers<void>();
	const timer = setTimeout(resolve, ms);
	const onAbort = (): void => reject(signal.reason);
	signal.addEventListener("abort", onAbort, { once: true });
	try {
		await promise;
	} finally {
		clearTimeout(timer);
		signal.removeEventListener("abort", onAbort);
	}
}
/**
 * Best-effort startup deadline for the optional Streamable HTTP GET SSE listener.
 *
 * Returns `0` (disabled) when the operator has explicitly disabled MCP client-side
 * timeouts via `timeout: 0` or `OMP_MCP_TIMEOUT_MS=0`, mirroring the rest of the
 * MCP timeout surface. Otherwise caps the wait at one second and scales below
 * short request timeouts so connect-time never exceeds the request budget.
 */
export function resolveSSEConnectTimeoutMs(configTimeout?: number): number {
	const requestTimeout = resolveMCPTimeoutMs(configTimeout);
	if (!isMCPTimeoutEnabled(requestTimeout)) return 0;
	const boundedTimeout = Math.min(HTTP_SSE_CONNECT_TIMEOUT_MS, Math.floor(requestTimeout / 4));
	return Math.max(1, boundedTimeout);
}
/**
 * HTTP transport for MCP servers.
 * Uses POST for requests, supports SSE responses.
 */
export class HttpTransport implements MCPTransport {
	#connected = false;
	#sessionId: string | null = null;
	#sseConnection: AbortController | null = null;
	readonly #requestIds = new RequestIdAllocator();
	#lifecycleController = new AbortController();
	readonly #activeRequests = new Set<Promise<unknown>>();
	readonly #activeFetches = new Set<Promise<Response>>();
	readonly #backgroundDrains = new Set<Promise<void>>();
	#closePromise: Promise<void> | null = null;
	#pendingAdmissions = 0;
	readonly #observeAdmission?: (pending: boolean) => void;
	#authResolutions?: Set<Promise<unknown>>;
	readonly #observeAuthResolution?: (pending: Promise<unknown>) => void;
	/**
	 * Protocol version echoed in the `MCP-Protocol-Version` header. `null` until
	 * the `initialize` response is negotiated (via {@link setProtocolVersion}):
	 * the MCP spec requires the header only on requests *after* `initialize`, and
	 * a server that supports only an older revision may reject a header carrying
	 * a newer version sent before negotiation completes.
	 */
	#protocolVersion: string | null = null;

	onClose?: () => void;
	onError?: (error: Error) => void;
	onNotification?: (method: string, params: unknown) => void;
	onRequest?: (method: string, params: unknown) => Promise<unknown>;
	/** Called on 401/403 to attempt token refresh. Returns updated headers or null. */
	onAuthError?: (signal?: AbortSignal) => Promise<Record<string, string> | null>;

	constructor(
		private config: MCPHttpServerConfig | MCPSseServerConfig,
		private readonly transportOptions?: MCPTransportOptions,
		private readonly connectSignal?: AbortSignal,
	) {
		if (
			transportOptions?.resolveAuthHeaders &&
			(config.type !== "http" ||
				config.headerPolicy !== "origin-locked" ||
				config.auth ||
				config.oauth ||
				!transportOptions.beforeSend)
		)
			throw new MCPAdmissionError();
		if (transportOptions?.resolveAuthHeaders) {
			this.#authResolutions = new Set();
			this.#observeAuthResolution = pending => {
				this.#authResolutions!.add(pending);
				void pending.then(
					() => this.#authResolutions!.delete(pending),
					() => this.#authResolutions!.delete(pending),
				);
			};
		}
		bindMCPTransport(this, transportOptions);
		if (transportOptions?.beforeSend && config.headerPolicy !== "origin-locked") throw new MCPAdmissionError();
		if (transportOptions?.beforeSend) {
			this.transportOptions = Object.freeze({
				beforeSend: transportOptions.beforeSend,
				signal: transportOptions.signal,
				...(transportOptions.resolveAuthHeaders
					? { resolveAuthHeaders: transportOptions.resolveAuthHeaders }
					: undefined),
			});
			this.#observeAdmission = pending => {
				this.#pendingAdmissions += pending ? 1 : -1;
			};
		}
	}

	#awaitAuthRefresh(signal: AbortSignal | undefined, refresh: () => Promise<Record<string, string> | null>) {
		const waiting = untilAborted(signal, refresh);
		if (!this.transportOptions?.beforeSend) return waiting;
		return waiting.catch(error => {
			if (isMCPAdmissionError(error)) throw error;
			const owner = this.transportOptions?.signal;
			if (owner?.aborted && isMCPAdmissionError(owner.reason)) throw owner.reason;
			if (signal?.aborted && isMCPAdmissionError(signal.reason)) throw signal.reason;
			throw error;
		});
	}

	/** Guarded diagnostics never retain server echoes, URLs or exception causes. */
	#safeError(error: unknown): Error {
		if (!this.transportOptions?.beforeSend || isMCPAdmissionError(error) || error instanceof MCPTransportError)
			return error instanceof Error ? error : new Error(String(error));
		return new Error("MCP HTTP operation failed");
	}

	#normalizeError(error: unknown, stage: MCPFailureStage, traceId?: string): MCPTransportError {
		const normalized = normalizeMCPTransportError(error, { transport: "http", stage, traceId });
		if (!this.transportOptions?.beforeSend || isMCPAdmissionError(normalized) || error instanceof MCPTransportError)
			return normalized;
		return new MCPTransportError({
			transport: "http",
			stage: normalized.stage,
			failure: normalized.failure,
			message: "MCP HTTP operation failed",
			retryable: normalized.retryable,
			code: typeof normalized.code === "number" ? normalized.code : undefined,
		});
	}

	/**
	 * Fetch the configured endpoint with header precedence and origin policy.
	 *
	 * The transport fully owns `MCP-Protocol-Version`: it is stripped from
	 * configured headers so a user's `mcp.json` can never inject it, and added
	 * only once a version is negotiated (required by the MCP Streamable HTTP spec
	 * after `initialize`). Before negotiation — the `initialize` request itself —
	 * no protocol-version header is sent from either source.
	 */
	#fetch(init: MCPFetchInit, generated: Record<string, string>, custody?: MCPSendCustody): Promise<Response> {
		if (isGuardedMCPTransport(this) && !custody)
			custody = createMCPControlCustody(
				this,
				init.method === "DELETE" ? "cleanup" : "maintenance",
				undefined,
				init.body,
			);
		const configured = withoutHeader(this.config.headers, "MCP-Protocol-Version");
		const withVersion =
			this.#protocolVersion === null ? generated : { "MCP-Protocol-Version": this.#protocolVersion, ...generated };
		const request = mcpFetch(
			this.config.url,
			init,
			{ generated: withVersion, configured },
			this.config.headerPolicy === "origin-locked",
			this.transportOptions,
			"http",
			this.#observeAdmission,
			undefined,
			custody,
			this.#observeAuthResolution,
		);
		this.#activeFetches.add(request);
		void request.then(
			() => this.#activeFetches.delete(request),
			() => this.#activeFetches.delete(request),
		);
		return request;
	}

	/** Combine caller cancellation with transport shutdown for every HTTP operation. */
	#operationSignal(signal?: AbortSignal): AbortSignal {
		const owner = this.transportOptions?.signal;
		if (owner)
			return AbortSignal.any(
				signal ? [signal, owner, this.#lifecycleController.signal] : [owner, this.#lifecycleController.signal],
			);
		return signal ? AbortSignal.any([signal, this.#lifecycleController.signal]) : this.#lifecycleController.signal;
	}

	/**
	 * Keep a rejection observer on public requests even if a timeout wrapper
	 * abandons the returned promise. The original promise is returned unchanged,
	 * so callers still observe its normal result or error.
	 */
	#trackRequest<T>(request: Promise<T>): Promise<T> {
		this.#activeRequests.add(request);
		void request.then(
			() => this.#activeRequests.delete(request),
			() => this.#activeRequests.delete(request),
		);
		return request;
	}

	/** Own a fire-and-forget body drain until it settles. */
	#trackBackgroundDrain(drain: Promise<void>): void {
		const handled = drain.catch(error => {
			if (error instanceof Error && error.name === "AbortError") return;
			logger.debug("MCP HTTP background drain failed", {
				url: this.transportOptions?.beforeSend ? undefined : this.config.url,
				error: this.#safeError(error).message,
			});
		});
		this.#backgroundDrains.add(handled);
		void handled.then(
			() => this.#backgroundDrains.delete(handled),
			() => this.#backgroundDrains.delete(handled),
		);
	}

	/** Record the protocol version negotiated during `initialize`. */
	setProtocolVersion(version: string): void {
		this.#protocolVersion = version;
	}

	get connected(): boolean {
		return this.#connected;
	}

	get url(): string {
		return this.config.url;
	}

	/**
	 * Mark transport as connected.
	 * HTTP doesn't need persistent connection, but we track state.
	 */
	async connect(): Promise<void> {
		this.connectSignal?.throwIfAborted();
		this.transportOptions?.signal?.throwIfAborted();
		if (this.#connected) return;
		if (this.#closePromise) await this.#closePromise;
		if (this.#lifecycleController.signal.aborted) {
			this.#lifecycleController = new AbortController();
		}
		this.#closePromise = null;
		this.#connected = true;
	}

	/**
	 * Start SSE listener for server-initiated messages.
	 * Resolves once the SSE connection is established (or fails/unsupported).
	 * Message reading continues in the background.
	 */
	async startSSEListener(): Promise<void> {
		if (!this.#connected) return;
		if (this.#sseConnection) return;

		this.#sseConnection = new AbortController();
		const generated: Record<string, string> = {
			Accept: "text/event-stream",
		};

		if (this.#sessionId) {
			generated["Mcp-Session-Id"] = this.#sessionId;
		}

		let response: Response | null;
		let timedOut = false;
		let startupFinished = false;
		const connection = this.#sseConnection;
		const startupTimeoutMs = resolveSSEConnectTimeoutMs(this.config.timeout);
		const fetchPromise = this.#fetch(
			{
				method: "GET",
				signal:
					this.transportOptions?.beforeSend || this.transportOptions?.signal
						? this.#operationSignal(connection.signal)
						: connection.signal,
			},
			generated,
		);
		const timeoutPromise =
			startupTimeoutMs > 0
				? new Promise<null>(resolve => {
						setTimeout(() => {
							if (!startupFinished) {
								timedOut = true;
								connection.abort();
							}
							resolve(null);
						}, startupTimeoutMs);
					})
				: null;
		try {
			response = timeoutPromise === null ? await fetchPromise : await Promise.race([fetchPromise, timeoutPromise]);
		} catch (error) {
			if (this.#sseConnection === connection) this.#sseConnection = null;
			if (isMCPAdmissionError(error)) throw error;
			if (this.transportOptions?.beforeSend && !timedOut && this.#operationSignal().aborted) throw error;
			if (error instanceof Error && error.name !== "AbortError" && !timedOut) {
				this.onError?.(this.#safeError(error));
			}
			return;
		} finally {
			startupFinished = true;
		}
		if (response === null) {
			if (this.#sseConnection === connection) this.#sseConnection = null;
			void fetchPromise.then(lateResponse => lateResponse.body?.cancel()).catch(() => {});
			return;
		}

		if (this.#sseConnection !== connection) {
			await response.body?.cancel();
			return;
		}
		if (response.status === 405 || !response.ok || !response.body) {
			await response.body?.cancel();
			if (this.#sseConnection === connection) this.#sseConnection = null;
			return;
		}

		// Connection established — read messages in background.
		// If the stream ends unexpectedly (server restart, network drop),
		// fire onClose so the manager can trigger reconnection.
		const signal = this.transportOptions?.beforeSend ? this.#operationSignal(connection.signal) : connection.signal;
		this.#trackBackgroundDrain(
			this.#runSSEListener(response.body, signal).finally(() => {
				const wasConnected = this.#connected;
				if (this.#sseConnection === connection) this.#sseConnection = null;
				if (wasConnected) this.onClose?.();
			}),
		);
	}
	async #readSSEStream(body: ReadableStream<Uint8Array>, signal: AbortSignal): Promise<void> {
		try {
			for await (const message of readSseJson<JsonRpcMessage>(body, signal)) {
				if (!this.#connected) break;
				this.#dispatchSSEMessage(message);
			}
		} catch (error) {
			if (error instanceof Error && error.name !== "AbortError") {
				logger.debug("HTTP SSE stream error", {
					url: this.transportOptions?.beforeSend ? undefined : this.config.url,
					error: this.#safeError(error).message,
				});
				this.onError?.(this.#safeError(error));
			}
		}
	}

	/**
	 * Read the long-lived GET SSE stream, resuming with `Last-Event-ID` when
	 * the server closes the physical connection mid-stream (2025-11-25 permits
	 * polling-style servers). Returns only when the logical stream ends — the
	 * caller fires `onClose` and the manager's reconnect path takes over. A
	 * resume cycle that delivers no events before dropping again ends the
	 * stream rather than retrying forever against a broken server.
	 */
	async #runSSEListener(initialBody: ReadableStream<Uint8Array>, signal: AbortSignal): Promise<void> {
		const resume: SSEResumeState = { lastEventId: null, retryMs: DEFAULT_SSE_RETRY_MS };
		let body = initialBody;
		let progressed = true;
		for (;;) {
			try {
				for await (const event of readSseEvents(body, signal)) {
					progressed = true;
					if (event.id !== undefined) resume.lastEventId = event.id || null;
					if (event.retry !== undefined) resume.retryMs = event.retry;
					if (event.data === "") continue;
					if (!this.#connected) return;
					this.#dispatchSSEMessage(JSON.parse(event.data) as JsonRpcMessage | JsonRpcMessage[]);
				}
			} catch (error) {
				if (error instanceof Error && error.name === "AbortError") return;
				logger.debug("HTTP SSE stream error", {
					url: this.transportOptions?.beforeSend ? undefined : this.config.url,
					error: this.#safeError(error).message,
				});
				if (resume.lastEventId === null) {
					if (error instanceof Error) this.onError?.(this.#safeError(error));
					return;
				}
			}
			if (!this.#connected || signal.aborted || resume.lastEventId === null || !progressed) return;
			progressed = false;
			try {
				const response = await this.#fetchSSEResume(resume, signal);
				body = response.body as ReadableStream<Uint8Array>;
			} catch (error) {
				if (isMCPAdmissionError(error)) {
					this.onClose = undefined;
					this.onError?.(error);
					return;
				}
				if (!(error instanceof Error && error.name === "AbortError")) {
					logger.debug("HTTP SSE listener resume failed", {
						url: this.transportOptions?.beforeSend ? undefined : this.config.url,
						error: this.#safeError(error).message,
					});
				}
				return;
			}
		}
	}

	/**
	 * Resume a logical SSE stream via GET + `Last-Event-ID`, honoring the
	 * server-provided retry interval and refreshing auth once on 401/403.
	 * Failures throw {@link SSEResumeError} so `request()` never replays the
	 * originating POST in response.
	 */
	async #fetchSSEResume(resume: SSEResumeState, signal: AbortSignal): Promise<Response> {
		if (resume.lastEventId === null) {
			throw new SSEResumeError("SSE stream ended without a resumable event ID");
		}
		await waitForSSERetry(resume.retryMs, signal);
		const generated: Record<string, string> = {
			Accept: "text/event-stream",
			"Last-Event-ID": resume.lastEventId,
		};
		if (this.#sessionId) generated["Mcp-Session-Id"] = this.#sessionId;
		let response = await this.#fetch({ method: "GET", signal }, generated);
		const refreshAuth = this.onAuthError;
		if (
			refreshAuth &&
			!this.transportOptions?.resolveAuthHeaders &&
			(response.status === 401 || response.status === 403)
		) {
			await response.body?.cancel();
			const newHeaders = await this.#awaitAuthRefresh(signal, () =>
				this.transportOptions?.beforeSend ? refreshAuth.call(this, signal) : refreshAuth.call(this),
			);
			if (this.transportOptions?.beforeSend) signal.throwIfAborted();
			if (!newHeaders) {
				throw new SSEResumeError(`HTTP ${response.status} resuming MCP SSE stream: auth refresh failed`);
			}
			// Persist refreshed headers so subsequent requests use them directly
			this.config = { ...this.config, headers: newHeaders };
			response = await this.#fetch({ method: "GET", signal }, generated);
		}
		if (!response.ok) {
			const text = await response.text().catch(() => {
				signal.throwIfAborted();
				return "";
			});
			throw new SSEResumeError(
				this.transportOptions?.beforeSend
					? `HTTP ${response.status} resuming MCP SSE stream`
					: `HTTP ${response.status} resuming MCP SSE stream: ${text}`,
			);
		}
		const contentType = response.headers.get("Content-Type") ?? "";
		if (!contentType.includes("text/event-stream") || !response.body) {
			await response.body?.cancel();
			throw new SSEResumeError(
				this.transportOptions?.beforeSend
					? "MCP SSE resume returned unsupported Content-Type"
					: `MCP SSE resume returned unsupported Content-Type: ${contentType || "(missing)"}`,
			);
		}
		return response;
	}

	/** Route an SSE message (or batch) to the appropriate handler. */
	#dispatchSSEMessage(message: JsonRpcMessage | JsonRpcMessage[]): void {
		if (Array.isArray(message)) {
			for (const m of message) this.#dispatchSSEMessage(m);
			return;
		}
		// Server-to-client request: has both method and id
		if ("method" in message && "id" in message && message.id != null) {
			if (this.transportOptions?.beforeSend) {
				this.#trackBackgroundDrain(
					this.#handleServerRequest(message as JsonRpcRequest).catch(error => {
						this.onError?.(this.#safeError(error));
					}),
				);
			} else void this.#handleServerRequest(message as JsonRpcRequest);
			return;
		}
		// Notification: has method but no id
		if ("method" in message && !("id" in message)) {
			this.onNotification?.(message.method, message.params);
		}
	}

	request<T = unknown>(method: string, params?: Record<string, unknown>, options?: MCPRequestOptions): Promise<T> {
		return this.#trackRequest(
			this.transportOptions?.beforeSend
				? this.#requestWithAuthRetry<T>(method, params, options)
				: this.#requestAttempts<T>(method, params, options),
		);
	}

	async #requestWithAuthRetry<T>(
		method: string,
		params: Record<string, unknown> | undefined,
		options: MCPRequestOptions | undefined,
	): Promise<T> {
		// One native deadline covers admission, the first POST, native auth and
		// replay. Detach caller cancellation at SSE capture, not at a 401.
		let callerSignal: AbortSignal | undefined;
		let releaseCaller: (() => void) | undefined;
		if (options?.signal) {
			const source = options.signal;
			const controller = new AbortController();
			const forwardAbort = (): void => controller.abort(source.reason);
			if (source.aborted) forwardAbort();
			else {
				source.addEventListener("abort", forwardAbort, { once: true });
				releaseCaller = () => source.removeEventListener("abort", forwardAbort);
			}
			callerSignal = controller.signal;
		}
		const timeout = resolveMCPTimeoutMs(this.config.timeout);
		const operation = createMCPTimeout(timeout, this.#operationSignal(callerSignal));
		try {
			return await this.#requestAttempts<T>(method, params, options, { operation, timeout, releaseCaller });
		} catch (error) {
			if (isMCPAdmissionError(error)) throw error;
			if (operation.timedOut()) {
				if (error instanceof MCPTransportError && error.failure === "timeout") throw error;
				throw new MCPTransportError({
					transport: "http",
					stage: "send",
					failure: "timeout",
					message: `Request timeout after ${timeout}ms`,
					retryable: false,
				});
			}
			throw error;
		} finally {
			releaseCaller?.();
			operation.clear();
		}
	}

	async #requestAttempts<T>(
		method: string,
		params: Record<string, unknown> | undefined,
		options: MCPRequestOptions | undefined,
		logical?: GuardedRequestOperation,
	): Promise<T> {
		try {
			return await this.#executeRequest<T>(method, params, options, logical);
		} catch (error) {
			// Retry once on auth failure if onAuthError is wired. Never replay
			// after an SSE resume failure: the server already accepted the
			// original POST and may have executed it — replaying could run a
			// state-changing tool twice.
			const status = error instanceof Error ? AIError.status(error) : undefined;
			if (
				!isMCPAdmissionError(error) &&
				!(error instanceof SSEResumeError) &&
				this.onAuthError &&
				!this.transportOptions?.resolveAuthHeaders &&
				(status === 401 || status === 403)
			) {
				const refreshSignal = logical?.operation.signal;
				const newHeaders = this.transportOptions?.beforeSend
					? await this.#awaitAuthRefresh(refreshSignal, () => this.onAuthError!(refreshSignal)).catch(
							refreshError => {
								if (isMCPAdmissionError(refreshError) || refreshSignal?.aborted) throw refreshError;
								throw this.#normalizeError(refreshError, "send");
							},
						)
					: await this.onAuthError();
				logical?.operation.signal?.throwIfAborted();
				if (newHeaders) {
					// Persist refreshed headers so subsequent requests use them directly
					this.config = { ...this.config, headers: newHeaders };
					return this.#executeRequest<T>(method, params, options, logical);
				}
			}
			throw error;
		}
	}

	async #executeRequest<T>(
		method: string,
		params: Record<string, unknown> | undefined,
		options: MCPRequestOptions | undefined,
		logical?: GuardedRequestOperation,
	): Promise<T> {
		if (!this.#connected) {
			throw new MCPTransportError({
				transport: "http",
				stage: "connect",
				failure: "closed",
				message: "Transport not connected",
				retryable: true,
			});
		}

		const id = this.#requestIds.next(this.config.requestIdFormat);
		const body = {
			jsonrpc: "2.0" as const,
			id,
			method,
			params: params ?? {},
		};
		const serialized = JSON.stringify(body);
		const custody = isGuardedMCPTransport(this)
			? createMCPRequestCustody(this, method, options, id, serialized)
			: undefined;

		const generated: Record<string, string> = {
			"Content-Type": "application/json",
			Accept: "application/json, text/event-stream",
		};

		if (this.#sessionId) {
			generated["Mcp-Session-Id"] = this.#sessionId;
		}

		// Caller cancellation owns this request only until its response arrives;
		// transport cancellation must continue owning any subsequent SSE messages.
		let callerSignal: AbortSignal | undefined;
		let releaseCaller = logical?.releaseCaller;
		if (!logical && options?.signal) {
			const source = options.signal;
			const controller = new AbortController();
			const forwardAbort = (): void => controller.abort(source.reason);
			if (source.aborted) forwardAbort();
			else {
				source.addEventListener("abort", forwardAbort, { once: true });
				releaseCaller = () => source.removeEventListener("abort", forwardAbort);
			}
			callerSignal = controller.signal;
		}
		const timeout = logical?.timeout ?? resolveMCPTimeoutMs(this.config.timeout);
		const operation = logical?.operation ?? createMCPTimeout(timeout, this.#operationSignal(callerSignal));
		let stage: MCPFailureStage = "send";
		let traceId: string | undefined;

		try {
			const response = await this.#fetch(
				{ method: "POST", body: serialized, signal: operation.signal },
				generated,
				custody,
			);
			stage = "receive";
			traceId = this.transportOptions?.beforeSend ? undefined : mcpTraceIdFromHeaders(response.headers);

			// Check for session ID in response
			const newSessionId = response.headers.get("Mcp-Session-Id");
			if (newSessionId) {
				this.#sessionId = newSessionId;
			}

			if (!response.ok) {
				const text = await response.text();
				const wwwAuthenticate = response.headers.get("WWW-Authenticate");
				const mcpAuthServer = response.headers.get("Mcp-Auth-Server");
				const authHints = [
					wwwAuthenticate ? `WWW-Authenticate: ${wwwAuthenticate}` : null,
					mcpAuthServer ? `Mcp-Auth-Server: ${mcpAuthServer}` : null,
				]
					.filter(Boolean)
					.join("; ");
				const suffix = authHints ? ` [${authHints}]` : "";
				const failure = new MCPTransportError({
					transport: "http",
					stage,
					failure: "http_status",
					message: this.transportOptions?.beforeSend
						? `HTTP ${response.status}: MCP request failed`
						: `HTTP ${response.status}: ${text}${suffix}`,
					retryable: response.status === 404 || response.status === 502 || response.status === 503,
					code: response.status,
					traceId,
				});
				if (this.transportOptions?.beforeSend && (response.status === 401 || response.status === 403)) {
					guardedAuthChallenges ??= new WeakMap();
					guardedAuthChallenges.set(failure, new Error(`HTTP ${response.status}: ${text}${suffix}`));
				}
				throw failure;
			}

			const contentType = response.headers.get("Content-Type") ?? "";

			// Handle SSE response. Await it here so the fetch and stream parser
			// share one logical deadline; returning the promise would let finally
			// clear the fetch timer before the response body is read.
			if (contentType.includes("text/event-stream")) {
				return await this.#parseSSEResponse<T>(response, id, operation, timeout, releaseCaller);
			}

			stage = "decode";
			// Handle JSON response
			const result: unknown = await response.json();
			if (
				!isRecord(result) ||
				result.jsonrpc !== "2.0" ||
				result.id !== id ||
				(!("result" in result) && !("error" in result))
			) {
				throw new SyntaxError("Malformed JSON-RPC response");
			}
			if (result.error !== undefined) {
				if (
					!isRecord(result.error) ||
					typeof result.error.code !== "number" ||
					typeof result.error.message !== "string"
				) {
					throw new SyntaxError("Malformed JSON-RPC error response");
				}
				throw createMCPJsonRpcError(
					"http",
					this.transportOptions?.beforeSend
						? { code: result.error.code, message: "MCP server error" }
						: { code: result.error.code, message: result.error.message, data: result.error.data },
					traceId,
				);
			}

			return result.result as T;
		} catch (error) {
			if (isMCPAdmissionError(error) || error instanceof SSEResumeError) throw error;
			if (operation.signal?.aborted && !operation.timedOut()) throw error;
			if (operation.isTimeoutAbort(error) || operation.timedOut()) {
				throw new MCPTransportError({
					transport: "http",
					stage,
					failure: "timeout",
					message: `Request timeout after ${timeout}ms`,
					retryable: false,
					traceId,
					cause: this.transportOptions?.beforeSend ? undefined : error,
				});
			}
			if (error instanceof Error && error.name === "AbortError") throw error;
			throw this.#normalizeError(error, stage, traceId);
		} finally {
			if (!logical) {
				releaseCaller?.();
				operation.clear();
			}
		}
	}

	#parseSSEResponse<T>(
		response: Response,
		expectedId: string | number,
		operation: MCPTimeoutOperation,
		timeout: number,
		releaseCaller?: () => void,
	): Promise<T> {
		const traceId = this.transportOptions?.beforeSend ? undefined : mcpTraceIdFromHeaders(response.headers);
		if (!response.body) {
			throw new MCPTransportError({
				transport: "http",
				stage: "decode",
				failure: "malformed_response",
				message: "SSE response did not include a body",
				retryable: false,
				traceId,
			});
		}

		const signal = operation.signal ?? getNeverAbortSignal();

		const { promise, resolve, reject } = Promise.withResolvers<T>();
		// The transport owns this promise until the physical stream drain exits.
		// Keep a rejection observer attached even when a caller-side timeout
		// abandons the request before the drain notices its abort.
		void promise.catch(() => {});
		const resume: SSEResumeState = { lastEventId: null, retryMs: DEFAULT_SSE_RETRY_MS };
		let captured = false;

		// Drain each physical SSE connection without leaving its iterator early.
		// A server may close a connection without terminating the logical stream;
		// when it supplied an event ID, resume that stream via GET + Last-Event-ID.
		const drain = async (): Promise<void> => {
			let current = response;
			try {
				for (;;) {
					if (!current.body) throw new Error("SSE response did not include a body");
					try {
						for await (const event of readSseEvents(current.body, signal)) {
							if (event.id !== undefined) resume.lastEventId = event.id || null;
							if (event.retry !== undefined) resume.retryMs = event.retry;
							if (event.data === "") continue;
							const raw = JSON.parse(event.data) as JsonRpcMessage | JsonRpcMessage[];
							const messages = Array.isArray(raw) ? raw : [raw];
							for (const message of messages) {
								if (
									!captured &&
									"id" in message &&
									message.id === expectedId &&
									("result" in message || "error" in message)
								) {
									if (
										this.transportOptions?.beforeSend &&
										message.error !== undefined &&
										(!isRecord(message.error) ||
											!Number.isSafeInteger(message.error.code) ||
											typeof message.error.message !== "string")
									) {
										throw new MCPTransportError({
											transport: "http",
											stage: "decode",
											failure: "malformed_response",
											message: "Malformed JSON-RPC error response",
											retryable: false,
										});
									}
									captured = true;
									releaseCaller?.();
									releaseCaller = undefined;
									operation.clear();
									if (message.error) {
										reject(
											createMCPJsonRpcError(
												"http",
												this.transportOptions?.beforeSend
													? { code: message.error.code, message: "MCP server error" }
													: message.error,
												traceId,
											),
										);
									} else {
										resolve(message.result as T);
									}
									continue;
								}
								if (!this.#connected) continue;
								this.#dispatchSSEMessage(message);
							}
						}
					} catch (error) {
						// An abrupt drop (socket reset, body-read failure) is as
						// resumable as a server-initiated close once an event ID
						// exists; the request timeout still bounds the total wait.
						if (captured) return;
						if (
							signal.aborted ||
							resume.lastEventId === null ||
							(error instanceof MCPTransportError && error.failure === "malformed_response")
						)
							throw error;
						logger.debug("MCP SSE response stream dropped; resuming", {
							url: this.transportOptions?.beforeSend ? undefined : this.config.url,
							error: this.#safeError(error).message,
						});
					}
					if (captured) return;
					if (signal.aborted) {
						throw signal.reason ?? new DOMException("MCP SSE response aborted", "AbortError");
					}
					if (resume.lastEventId === null) {
						throw new MCPTransportError({
							transport: "http",
							stage: "receive",
							failure: "eof",
							message: `No response received for request ID ${expectedId}`,
							retryable: false,
							traceId,
						});
					}
					current = await this.#fetchSSEResume(resume, signal);
				}
			} catch (error) {
				if (captured) return;
				// The server accepted this POST (it returned a 2xx SSE stream) before
				// the drain or a resume GET failed, so the originating request must
				// never be replayed — it may already have executed a state-changing
				// tool. Preserve SSEResumeError so #requestWithAuthRetry's no-replay
				// guard still fires instead of refreshing auth and re-POSTing, and
				// force every other post-acceptance failure non-retryable so the
				// reconnect path in isRetriableConnectionError cannot replay it.
				if (
					isMCPAdmissionError(error) ||
					error instanceof SSEResumeError ||
					(signal.aborted && !operation.timedOut())
				) {
					reject(error);
				} else if (operation.isTimeoutAbort(error) || operation.timedOut()) {
					reject(
						new MCPTransportError({
							transport: "http",
							stage: "receive",
							failure: "timeout",
							message: `SSE response timeout after ${timeout}ms`,
							retryable: false,
							traceId,
							cause: this.transportOptions?.beforeSend ? undefined : error,
						}),
					);
				} else if (error instanceof Error && error.name === "AbortError") {
					reject(error);
				} else {
					const normalized = this.#normalizeError(
						error,
						error instanceof SyntaxError ? "decode" : "receive",
						traceId,
					);
					reject(
						normalized.retryable
							? new MCPTransportError({
									transport: normalized.transport,
									stage: normalized.stage,
									failure: normalized.failure,
									message: normalized.message,
									retryable: false,
									code: normalized.code,
									data: normalized.data,
									traceId: normalized.traceId,
									cause: normalized,
								})
							: normalized,
					);
				}
			} finally {
				operation.clear();
				await current.body?.cancel().catch(() => {});
			}
		};

		this.#trackBackgroundDrain(drain());
		return promise;
	}

	async #handleServerRequest(request: JsonRpcRequest): Promise<void> {
		if (isGuardedMCPTransport(this)) {
			await this.#sendServerResponse(request.id, undefined, { code: -32601, message: "Unsupported server request" });
			return;
		}
		if (!this.onRequest) {
			await this.#sendServerResponse(request.id, undefined, { code: -32601, message: "Method not found" });
			return;
		}
		let result: unknown;
		try {
			result = this.transportOptions?.beforeSend
				? await untilAborted(this.#operationSignal(), () => this.onRequest!(request.method, request.params))
				: await this.onRequest(request.method, request.params);
		} catch (error) {
			if (this.transportOptions?.beforeSend && (isMCPAdmissionError(error) || this.#operationSignal().aborted))
				throw error;
			await this.#sendServerResponse(request.id, undefined, toJsonRpcError(error));
			return;
		}
		await this.#sendServerResponse(request.id, result);
	}

	/** POST a JSON-RPC response back to the server (for server-to-client requests received via SSE). */
	async #sendServerResponse(id: string | number, result?: unknown, error?: JsonRpcError): Promise<void> {
		if (!this.#connected) return;
		const body = error
			? { jsonrpc: "2.0" as const, id, error }
			: { jsonrpc: "2.0" as const, id, result: result ?? {} };
		const generated: Record<string, string> = {
			"Content-Type": "application/json",
			Accept: "application/json, text/event-stream",
		};
		if (this.#sessionId) {
			generated["Mcp-Session-Id"] = this.#sessionId;
		}
		const payload = JSON.stringify(body);
		const timeout = resolveMCPTimeoutMs(this.config.timeout);
		const operation = createMCPTimeout(timeout, this.#operationSignal());
		try {
			const resp = await this.#fetch(
				{ method: "POST", body: payload, signal: operation.signal },
				generated,
				isGuardedMCPTransport(this) ? createMCPControlCustody(this, "server-response", id, payload) : undefined,
			);
			// Retry once on auth failure if onAuthError is wired
			if (
				this.onAuthError &&
				!this.transportOptions?.resolveAuthHeaders &&
				(resp.status === 401 || resp.status === 403)
			) {
				await resp.body?.cancel();
				const newHeaders = this.transportOptions?.beforeSend
					? await this.#awaitAuthRefresh(operation.signal, () => this.onAuthError!(operation.signal))
					: await this.onAuthError();
				if (newHeaders) {
					this.config.headers ??= {};
					Object.assign(this.config.headers, newHeaders);
					if (!this.transportOptions?.beforeSend) operation.clear();
					const retryOperation = this.transportOptions?.beforeSend
						? operation
						: createMCPTimeout(timeout, this.#operationSignal());
					try {
						const retry = await this.#fetch(
							{ method: "POST", body: payload, signal: retryOperation.signal },
							generated,
						);
						await retry.body?.cancel();
					} finally {
						retryOperation.clear();
					}
					return;
				}
			}
			await resp.body?.cancel();
		} catch (error) {
			if (this.transportOptions?.beforeSend)
				throw operation.signal?.aborted || isMCPAdmissionError(error) ? error : this.#safeError(error);
			// Best-effort response delivery — server may have disconnected
		} finally {
			operation.clear();
		}
	}

	notify(method: string, params?: Record<string, unknown>): Promise<void> {
		return this.#trackRequest(this.#sendNotification(method, params));
	}

	async #sendNotification(method: string, params?: Record<string, unknown>): Promise<void> {
		if (isGuardedMCPTransport(this) && method !== "notifications/initialized") throw new MCPAdmissionError();
		if (!this.#connected) {
			throw new MCPTransportError({
				transport: "http",
				stage: "connect",
				failure: "closed",
				message: "Transport not connected",
				retryable: true,
			});
		}

		const body = {
			jsonrpc: "2.0" as const,
			method,
			params: params ?? {},
		};

		const generated: Record<string, string> = {
			"Content-Type": "application/json",
			Accept: "application/json, text/event-stream",
		};

		if (this.#sessionId) {
			generated["Mcp-Session-Id"] = this.#sessionId;
		}

		const serialized = JSON.stringify(body);
		const custody = isGuardedMCPTransport(this)
			? createMCPControlCustody(this, "startup", undefined, serialized)
			: undefined;
		const timeout = resolveMCPTimeoutMs(this.config.timeout);
		const operation = createMCPTimeout(timeout, this.#operationSignal());
		let stage: MCPFailureStage = "send";
		let traceId: string | undefined;

		try {
			const response = await this.#fetch(
				{ method: "POST", body: serialized, signal: operation.signal },
				generated,
				custody,
			);
			stage = "receive";
			traceId = this.transportOptions?.beforeSend ? undefined : mcpTraceIdFromHeaders(response.headers);

			// 202 Accepted is success for notifications
			if (!response.ok && response.status !== 202) {
				const text = await response.text();
				throw new MCPTransportError({
					transport: "http",
					stage,
					failure: "http_status",
					message: this.transportOptions?.beforeSend
						? `HTTP ${response.status}: MCP request failed`
						: `HTTP ${response.status}: ${text}`,
					retryable: response.status === 404 || response.status === 502 || response.status === 503,
					code: response.status,
					traceId,
				});
			}

			// The server may piggyback server-to-client requests or notifications
			// on the notification response (MCP Streamable HTTP spec). Read them.
			const contentType = response.headers.get("Content-Type") ?? "";
			if (contentType.includes("text/event-stream") && response.body) {
				// A successful notification POST has been accepted. Its SSE body is
				// now a background server-message stream, not part of the request
				// deadline; keep draining until its connection or the transport closes.
				const signal = this.#sseConnection
					? this.#operationSignal(this.#sseConnection.signal)
					: this.transportOptions?.beforeSend
						? this.#operationSignal()
						: this.#lifecycleController.signal;
				this.#trackBackgroundDrain(this.#readSSEStream(response.body, signal));
			} else {
				await response.body?.cancel();
			}
		} catch (error) {
			if (isMCPAdmissionError(error)) throw error;
			if (operation.signal?.aborted && !operation.timedOut()) throw error;
			if (operation.isTimeoutAbort(error) || operation.timedOut()) {
				throw new MCPTransportError({
					transport: "http",
					stage,
					failure: "timeout",
					message: `Notify timeout after ${timeout}ms`,
					retryable: false,
					traceId,
					cause: this.transportOptions?.beforeSend ? undefined : error,
				});
			}
			if (error instanceof Error && error.name === "AbortError") throw error;
			throw this.#normalizeError(error, stage, traceId);
		} finally {
			operation.clear();
		}
	}

	close(): Promise<void> {
		if (this.#closePromise) return this.#closePromise;
		if (!this.#connected) return Promise.resolve();
		this.#closePromise = this.#closeTransport();
		// `close()` is commonly fire-and-forget during process teardown.
		void this.#closePromise.catch(() => {});
		return this.#closePromise;
	}

	async #closeTransport(): Promise<void> {
		this.#connected = false;
		const closeReason = postmortem.markExpectedCleanupError(
			new DOMException("MCP HTTP transport closed", "AbortError"),
		);
		this.#lifecycleController.abort(closeReason);

		if (this.#sseConnection) {
			this.#sseConnection.abort(closeReason);
			this.#sseConnection = null;
		}

		// Aborting is only the cancellation request. Wait until fetches and body
		// readers have actually observed it before session/process teardown can
		// close their sockets underneath still-running promise continuations.
		while (
			this.#activeFetches.size > 0 ||
			this.#activeRequests.size > 0 ||
			this.#backgroundDrains.size > 0 ||
			(this.#authResolutions?.size ?? 0) > 0
		) {
			await Promise.allSettled([...this.#activeFetches, ...this.#activeRequests, ...this.#backgroundDrains]);
			if (this.#authResolutions?.size) await Promise.allSettled(this.#authResolutions);
		}

		if (this.#pendingAdmissions > 0)
			logger.debug("MCP HTTP local cleanup complete; callback drain unconfirmed; remote work not confirmed stopped");
		let terminationError: Error | undefined;
		if (
			(isGuardedMCPTransport(this) || this.transportOptions?.resolveAuthHeaders) &&
			this.transportOptions?.signal?.aborted
		)
			this.#sessionId = null;
		if (isGuardedMCPTransport(this) && this.#pendingAdmissions > 0)
			terminationError = new Error("MCP admission callback drain is unconfirmed");
		if (this.#sessionId) {
			const timeout = resolveMCPTimeoutMs(this.config.timeout);
			const guarded = !!this.transportOptions?.beforeSend;
			// Termination has its own cleanup lifetime, but never borrows revoked owner permission.
			const cleanup = guarded ? new AbortController() : undefined;
			const cleanupSignal =
				cleanup && this.transportOptions?.signal
					? AbortSignal.any([cleanup.signal, this.transportOptions.signal])
					: cleanup?.signal;
			const operation = createMCPTimeout(timeout, cleanupSignal);
			try {
				const attempt = this.#fetch(
					{ method: "DELETE", signal: operation.signal },
					{ "Mcp-Session-Id": this.#sessionId },
				);
				// Disabled deadlines cannot make close depend on arbitrary callback settlement.
				// Invalidate this cleanup attempt rather than inventing a timeout.
				if (guarded && !isMCPTimeoutEnabled(timeout)) cleanup!.abort(closeReason);
				const response = await attempt;
				await response.body?.cancel();
				if (guarded && !response.ok)
					terminationError = new Error("MCP remote termination unconfirmed; callback drain unconfirmed");
			} catch (error) {
				if (guarded)
					terminationError = isMCPAdmissionError(error)
						? error
						: new Error("MCP remote termination unconfirmed; callback drain unconfirmed");
			} finally {
				operation.clear();
				this.#sessionId = null;
			}
		}

		const onClose = this.onClose;
		this.onClose = undefined;
		try {
			onClose?.();
		} catch (error) {
			logger.debug("MCP HTTP onClose callback failed during transport teardown", {
				url: this.transportOptions?.beforeSend ? undefined : this.config.url,
				error: this.#safeError(error).message,
			});
		}
		if (terminationError) {
			logger.debug("MCP remote termination unconfirmed; callback drain unconfirmed");
			throw terminationError;
		}
	}
}

/**
 * Create and connect an HTTP transport.
 */
export async function createHttpTransport(
	config: MCPHttpServerConfig | MCPSseServerConfig,
	transportOptions?: MCPTransportOptions,
	connectSignal?: AbortSignal,
): Promise<HttpTransport> {
	const transport = new HttpTransport(config, transportOptions, connectSignal);
	try {
		await transport.connect();
		connectSignal?.throwIfAborted();
		transportOptions?.signal?.throwIfAborted();
		return transport;
	} catch (error) {
		await transport.close().catch(() => {});
		throw error;
	}
}
