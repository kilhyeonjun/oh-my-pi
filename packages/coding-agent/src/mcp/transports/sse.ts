import * as AIError from "@oh-my-pi/pi-ai/error";
import { logger, postmortem, readSseEvents, untilAborted } from "@oh-my-pi/pi-utils";
import type {
	JsonRpcError,
	JsonRpcMessage,
	JsonRpcRequest,
	JsonRpcResponse,
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
	MCPTransportError,
	normalizeMCPTransportError,
} from "../errors";
import { RequestIdAllocator } from "../request-id";
import { createMCPTimeout, getNeverAbortSignal, resolveMCPTimeoutMs } from "../timeout";
import { type MCPFetchInit, mcpFetch } from "./header-policy";

interface MCPTimeoutOperation {
	signal?: AbortSignal;
	clear: () => void;
	isTimeoutAbort: (error: unknown) => boolean;
}

interface PendingLegacySseRequest {
	resolve: (value: unknown) => void;
	reject: (reason?: unknown) => void;
	operation: MCPTimeoutOperation;
	abortHandler?: () => void;
	sent?: boolean;
}

/** Identifies a legacy SSE transport whose endpoint handshake timed out. */
export class LegacySseConnectionTimeoutError extends Error {
	constructor(timeoutMs: number) {
		super(`Legacy SSE endpoint timeout after ${timeoutMs}ms`);
		this.name = "LegacySseConnectionTimeoutError";
	}
}

/** Legacy MCP HTTP+SSE transport from protocol revision 2024-11-05. */
export class LegacySseTransport implements MCPTransport {
	#connected = false;
	#endpointUrl: string | null = null;
	#sseConnection: AbortController | null = null;
	#lifecycleController = new AbortController();
	#pending = new Map<string | number, PendingLegacySseRequest>();
	#config: MCPSseServerConfig;
	readonly #requestIds = new RequestIdAllocator();
	#pendingAdmissions = 0;
	readonly #observeAdmission?: (pending: boolean) => void;

	onClose?: () => void;
	onError?: (error: Error) => void;
	onNotification?: (method: string, params: unknown) => void;
	onRequest?: (method: string, params: unknown) => Promise<unknown>;
	/** Called on 401/403 to attempt token refresh. Returns updated headers or null. */
	onAuthError?: (signal?: AbortSignal) => Promise<Record<string, string> | null>;

	constructor(
		config: MCPSseServerConfig,
		private readonly transportOptions?: MCPTransportOptions,
	) {
		if (transportOptions?.resolveAuthHeaders) throw new MCPAdmissionError();
		if (transportOptions?.beforeSend && config.headerPolicy !== "origin-locked") throw new MCPAdmissionError();
		this.#config = config;
		if (transportOptions?.beforeSend) {
			this.transportOptions = Object.freeze({
				beforeSend: transportOptions.beforeSend,
				signal: transportOptions.signal,
			});
			this.#tasks = new Set();
			this.#observeAdmission = pending => {
				this.#pendingAdmissions += pending ? 1 : -1;
			};
		}
	}

	readonly #tasks: Set<Promise<unknown>> | undefined;
	#track<T>(task: Promise<T>): Promise<T> {
		if (!this.#tasks) return task;
		this.#tasks.add(task);
		void task.then(
			() => this.#tasks!.delete(task),
			() => this.#tasks!.delete(task),
		);
		return task;
	}

	#safeError(error: unknown): Error {
		return !this.transportOptions?.beforeSend || isMCPAdmissionError(error) || error instanceof MCPTransportError
			? error instanceof Error
				? error
				: new Error(String(error))
			: new Error("MCP legacy SSE operation failed");
	}

	/** Fetch an endpoint with header precedence and origin policy. */
	#fetch(url: string, init: MCPFetchInit, generated: Record<string, string>, onSend?: () => void): Promise<Response> {
		const request = mcpFetch(
			url,
			init,
			{ generated, configured: this.#config.headers },
			this.#config.headerPolicy === "origin-locked",
			this.transportOptions,
			"sse",
			this.#observeAdmission,
			onSend,
		);
		return this.transportOptions?.beforeSend
			? this.#track(
					request.catch(error => {
						if (init.signal?.aborted || isMCPAdmissionError(error)) throw error;
						const normalized = normalizeMCPTransportError(error, { transport: "http", stage: "send" });
						throw new MCPTransportError({
							transport: "http",
							stage: normalized.stage,
							failure: normalized.failure,
							message: "MCP legacy SSE operation failed",
							retryable: normalized.retryable,
							code: typeof normalized.code === "number" ? normalized.code : undefined,
						});
					}),
				)
			: request;
	}

	/**
	 * Combine caller cancellation with transport shutdown for every HTTP
	 * operation, so `close()` ends an in-flight POST as well as the GET stream.
	 * Deadlines remain configured-only: this composes cancellation, never a
	 * timer.
	 */
	#operationSignal(signal?: AbortSignal): AbortSignal {
		const owner = this.transportOptions?.signal;
		if (owner)
			return AbortSignal.any(
				signal ? [signal, owner, this.#lifecycleController.signal] : [owner, this.#lifecycleController.signal],
			);
		return signal ? AbortSignal.any([signal, this.#lifecycleController.signal]) : this.#lifecycleController.signal;
	}

	get connected(): boolean {
		return this.#connected;
	}

	get url(): string {
		return this.#config.url;
	}

	/** `signal` cancels only the wait for the endpoint event; an established stream outlives it. */
	async connect(signal?: AbortSignal): Promise<void> {
		if (this.#connected) return;
		if (this.#sseConnection) return;

		if (this.#lifecycleController.signal.aborted) {
			this.#lifecycleController = new AbortController();
		}
		signal?.throwIfAborted();
		const connection = new AbortController();
		const onAbort = () => connection.abort(signal?.reason);
		signal?.addEventListener("abort", onAbort, { once: true });
		const timeout = resolveMCPTimeoutMs(this.#config.timeout);
		const operation = createMCPTimeout(timeout, this.#operationSignal(connection.signal));
		const endpointReady = Promise.withResolvers<void>();
		this.#sseConnection = connection;

		try {
			const response = await this.#fetch(
				this.#config.url,
				{ method: "GET", signal: operation.signal },
				{ Accept: "text/event-stream" },
			);

			if (!response.ok) {
				const text = await response.text();
				throw new Error(
					this.transportOptions?.beforeSend
						? `HTTP ${response.status}: MCP request failed`
						: `HTTP ${response.status}: ${text}`,
				);
			}
			if (!response.body) {
				throw new Error("Legacy SSE response did not include a body");
			}

			const stream = this.#readSSEStream(response.body, operation, endpointReady).finally(() => {
				const wasConnected = this.#connected;
				if (this.#sseConnection === connection) this.#sseConnection = null;
				if (wasConnected) this.onClose?.();
			});
			if (this.transportOptions?.beforeSend)
				this.#track(
					stream.catch(error => {
						this.onError?.(this.#safeError(error));
					}),
				);
			else void stream;
			await endpointReady.promise;
		} catch (error) {
			operation.clear();
			if (this.#sseConnection === connection) this.#sseConnection = null;
			const aborted = operation.signal?.aborted;
			connection.abort();
			if (isMCPAdmissionError(error)) throw error;
			if (operation.isTimeoutAbort(error)) {
				throw new LegacySseConnectionTimeoutError(timeout);
			}
			throw this.transportOptions?.beforeSend && !aborted ? this.#safeError(error) : error;
		} finally {
			signal?.removeEventListener("abort", onAbort);
		}
	}

	async #readSSEStream(
		body: ReadableStream<Uint8Array>,
		operation: MCPTimeoutOperation,
		endpointReady: PromiseWithResolvers<void>,
	): Promise<void> {
		const signal = operation.signal ?? getNeverAbortSignal();
		let endpointReceived = false;
		try {
			for await (const event of readSseEvents(body, signal)) {
				if (event.event === "endpoint") {
					if (!this.#endpointUrl) {
						const endpointUrl = new URL(event.data, this.#config.url);
						const configuredUrl = new URL(this.#config.url);
						if (endpointUrl.origin !== configuredUrl.origin) {
							throw new Error(
								this.transportOptions?.beforeSend
									? "Legacy SSE endpoint origin mismatch"
									: `Legacy SSE endpoint origin mismatch: expected ${configuredUrl.origin}, received ${endpointUrl.origin}`,
							);
						}
						this.#endpointUrl = endpointUrl.href;
						this.#connected = true;
						endpointReceived = true;
						operation.clear();
						endpointReady.resolve();
					}
					continue;
				}
				if (event.data === "" || event.data === "[DONE]") continue;

				let payload: unknown;
				try {
					payload = JSON.parse(event.data) as unknown;
				} catch (error) {
					if (error instanceof SyntaxError) {
						throw new Error(
							this.transportOptions?.beforeSend
								? "Legacy SSE message contained non-JSON data"
								: `Legacy SSE message event contained non-JSON data: ${event.data}`,
						);
					}
					throw error;
				}

				const messages = Array.isArray(payload) ? payload : [payload];
				for (const message of messages) {
					if (typeof message !== "object" || message === null) continue;
					this.#dispatchMessage(message as JsonRpcMessage);
				}
			}
			if (!endpointReceived) {
				endpointReady.reject(new Error("Legacy SSE endpoint event not received"));
			}
		} catch (error) {
			if (!endpointReceived) {
				endpointReady.reject(error);
			} else if (error instanceof Error && error.name !== "AbortError") {
				logger.debug("Legacy SSE stream error", {
					url: this.transportOptions?.beforeSend ? undefined : this.#config.url,
					error: this.#safeError(error).message,
				});
				this.onError?.(this.#safeError(error));
				this.#rejectPending(this.#safeError(error));
			}
		} finally {
			operation.clear();
			if (endpointReceived) {
				this.#rejectPending(new Error("Transport closed: legacy SSE stream closed"));
			}
		}
	}

	#dispatchMessage(message: JsonRpcMessage): void {
		if ("id" in message && ("result" in message || "error" in message)) {
			const pending = this.#pending.get(message.id);
			if (pending) {
				// A matching id on the open stream is not evidence that this
				// request passed its POST admission boundary.
				if (this.transportOptions?.beforeSend && !pending.sent) return;
				this.#pending.delete(message.id);
				pending.operation.clear();
				if (pending.abortHandler) pending.operation.signal?.removeEventListener("abort", pending.abortHandler);
				const response = message as JsonRpcResponse;
				if (this.transportOptions?.beforeSend && "error" in response) {
					const error = response.error;
					pending.reject(
						typeof error === "object" &&
							error !== null &&
							Number.isSafeInteger(error.code) &&
							typeof error.message === "string"
							? createMCPJsonRpcError("http", { code: error.code, message: "MCP server error" })
							: new MCPTransportError({
									transport: "http",
									stage: "decode",
									failure: "malformed_response",
									message: "Malformed MCP error response",
									retryable: false,
								}),
					);
				} else if (response.error) {
					pending.reject(new Error(`MCP error ${response.error.code}: ${response.error.message}`));
				} else {
					pending.resolve(response.result);
				}
				return;
			}
		}
		if ("method" in message && "id" in message && message.id != null) {
			if (this.transportOptions?.beforeSend)
				this.#track(
					this.#handleServerRequest(message as JsonRpcRequest).catch(error => {
						this.onError?.(this.#safeError(error));
					}),
				);
			else void this.#handleServerRequest(message as JsonRpcRequest);
			return;
		}
		if ("method" in message && !("id" in message)) {
			this.onNotification?.(message.method, message.params);
		}
	}

	async request<T = unknown>(
		method: string,
		params?: Record<string, unknown>,
		options?: MCPRequestOptions,
	): Promise<T> {
		if (!this.#connected || !this.#endpointUrl) {
			throw new Error("Transport not connected");
		}

		const id = this.#requestIds.next(this.#config.requestIdFormat);
		const body = {
			jsonrpc: "2.0" as const,
			id,
			method,
			params: params ?? {},
		};
		const timeout = resolveMCPTimeoutMs(this.#config.timeout);
		const sending = this.transportOptions?.beforeSend ? new AbortController() : undefined;
		const callerSignal =
			sending && options?.signal
				? AbortSignal.any([sending.signal, options.signal])
				: (sending?.signal ?? options?.signal);
		const operation = createMCPTimeout(timeout, this.#operationSignal(callerSignal));
		const deferred = Promise.withResolvers<unknown>();
		if (sending)
			void deferred.promise.then(
				() => sending.abort(new DOMException("MCP request settled", "AbortError")),
				() => sending.abort(new DOMException("MCP request settled", "AbortError")),
			);
		// Observe the response promise synchronously so a stream-close rejection
		// from `#rejectPending` that lands while `request()` is still awaiting the
		// POST round-trip is never flagged as an unhandled rejection. The real
		// `await deferred.promise` below still receives and propagates the error.
		void deferred.promise.catch(() => undefined);
		const pending: PendingLegacySseRequest = {
			resolve: deferred.resolve,
			reject: deferred.reject,
			operation,
		};
		if (this.transportOptions?.beforeSend) pending.sent = false;
		if (operation.signal) {
			pending.abortHandler = () => {
				this.#pending.delete(id);
				operation.clear();
				// Name the source that actually aborted: the caller's reason, the
				// close reason, or — when neither signalled — the configured timer.
				const aborted = options?.signal?.aborted
					? options.signal.reason
					: this.transportOptions?.signal?.aborted
						? this.transportOptions.signal.reason
						: this.#lifecycleController.signal.aborted
							? this.#lifecycleController.signal.reason
							: undefined;
				deferred.reject(
					aborted instanceof Error ? aborted : new Error(`Legacy SSE response timeout after ${timeout}ms`),
				);
			};
			operation.signal.addEventListener("abort", pending.abortHandler, { once: true });
		}
		this.#pending.set(id, pending);

		if (!this.transportOptions?.beforeSend) {
			try {
				const response = await this.#postJson(body, operation.signal);
				if (!response.ok) throw new Error(`HTTP ${response.status}: ${await response.text()}`);
				await response.body?.cancel();
				return (await deferred.promise) as T;
			} catch (error) {
				this.#pending.delete(id);
				operation.clear();
				if (pending.abortHandler) operation.signal?.removeEventListener("abort", pending.abortHandler);
				if (operation.isTimeoutAbort(error)) throw new Error(`Request timeout after ${timeout}ms`);
				throw error;
			}
		}
		const send = async (): Promise<void> => {
			try {
				operation.signal?.throwIfAborted();
				const response = await this.#postJson(body, operation.signal, () => {
					operation.signal?.throwIfAborted();
					if (this.#pending.get(id) !== pending)
						throw new DOMException("MCP request is no longer pending", "AbortError");
					pending.sent = true;
				});
				if (!response.ok) {
					const text = await response.text();
					throw new Error(
						this.transportOptions?.beforeSend
							? `HTTP ${response.status}: MCP request failed`
							: `HTTP ${response.status}: ${text}`,
					);
				}
				await response.body?.cancel();
			} catch (error) {
				if (this.#pending.get(id) !== pending) return;
				this.#pending.delete(id);
				operation.clear();
				if (pending.abortHandler) operation.signal?.removeEventListener("abort", pending.abortHandler);
				const failure =
					!isMCPAdmissionError(error) && operation.isTimeoutAbort(error)
						? new Error(`Request timeout after ${timeout}ms`)
						: operation.signal?.aborted || isMCPAdmissionError(error)
							? error
							: this.#safeError(error);
				deferred.reject(failure);
			}
		};
		this.#track(send());
		return (await deferred.promise) as T;
	}

	async notify(method: string, params?: Record<string, unknown>): Promise<void> {
		if (!this.#connected || !this.#endpointUrl) {
			throw new Error("Transport not connected");
		}

		const timeout = resolveMCPTimeoutMs(this.#config.timeout);
		const operation = createMCPTimeout(timeout, this.#operationSignal());
		try {
			const response = await this.#postJson(
				{
					jsonrpc: "2.0" as const,
					method,
					params: params ?? {},
				},
				operation.signal,
			);
			operation.clear();
			if (!response.ok) {
				const text = await response.text();
				throw new Error(
					this.transportOptions?.beforeSend
						? `HTTP ${response.status}: MCP request failed`
						: `HTTP ${response.status}: ${text}`,
				);
			}
			await response.body?.cancel();
		} catch (error) {
			operation.clear();
			if (isMCPAdmissionError(error)) throw error;
			if (operation.isTimeoutAbort(error)) {
				throw new Error(`Notify timeout after ${timeout}ms`);
			}
			throw !this.transportOptions?.beforeSend || operation.signal?.aborted ? error : this.#safeError(error);
		}
	}

	async #postJson(
		body: JsonRpcRequest | JsonRpcResponse | { jsonrpc: "2.0"; method: string; params: Record<string, unknown> },
		signal?: AbortSignal,
		onSend?: () => void,
	): Promise<Response> {
		const endpointUrl = this.#endpointUrl;
		if (!endpointUrl) throw new Error("Transport not connected");
		const generated: Record<string, string> = {
			"Content-Type": "application/json",
			Accept: "application/json, text/event-stream",
		};
		const payload = JSON.stringify(body);
		let response = await this.#fetch(endpointUrl, { method: "POST", body: payload, signal }, generated, onSend);
		const status = AIError.status(response);
		if (!this.onAuthError || (status !== 401 && status !== 403)) return response;

		const refreshedHeaders = this.transportOptions?.beforeSend
			? await untilAborted(signal, () => this.onAuthError!(signal)).catch(error => {
					if (isMCPAdmissionError(error)) throw error;
					const owner = this.transportOptions?.signal;
					if (owner?.aborted && isMCPAdmissionError(owner.reason)) throw owner.reason;
					if (signal?.aborted && isMCPAdmissionError(signal.reason)) throw signal.reason;
					if (signal?.aborted) throw error;
					throw this.#safeError(error);
				})
			: await this.onAuthError();
		if (!refreshedHeaders) return response;
		await response.body?.cancel();
		if (this.transportOptions?.beforeSend) signal?.throwIfAborted();
		this.#config.headers = refreshedHeaders;
		response = await this.#fetch(endpointUrl, { method: "POST", body: payload, signal }, generated, onSend);
		return response;
	}

	async #handleServerRequest(request: JsonRpcRequest): Promise<void> {
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

	async #sendServerResponse(id: string | number, result?: unknown, error?: JsonRpcError): Promise<void> {
		if (!this.#connected) return;
		const timeout = resolveMCPTimeoutMs(this.#config.timeout);
		const operation = createMCPTimeout(timeout, this.#operationSignal());
		try {
			const response = await this.#postJson(
				error ? { jsonrpc: "2.0" as const, id, error } : { jsonrpc: "2.0" as const, id, result: result ?? {} },
				operation.signal,
			);
			operation.clear();
			await response.body?.cancel();
		} catch (error) {
			if (this.transportOptions?.beforeSend)
				throw operation.signal?.aborted || isMCPAdmissionError(error) ? error : this.#safeError(error);
		} finally {
			operation.clear();
		}
	}

	#rejectPending(error: Error): void {
		for (const [id, pending] of this.#pending) {
			this.#pending.delete(id);
			pending.operation.clear();
			if (pending.abortHandler) pending.operation.signal?.removeEventListener("abort", pending.abortHandler);
			pending.reject(error);
		}
	}

	async close(): Promise<void> {
		if (!this.#connected && !this.#sseConnection) return;
		const wasConnected = this.#connected;
		this.#connected = false;
		this.#endpointUrl = null;
		const closeReason = postmortem.markExpectedCleanupError(
			new DOMException("MCP legacy SSE transport closed", "AbortError"),
		);
		// Before rejecting pending entries, so an in-flight POST is cancelled
		// rather than left holding a socket, and each waiter is told the close
		// reason rather than a deadline it never had.
		this.#lifecycleController.abort(closeReason);
		if (this.#sseConnection) {
			this.#sseConnection.abort(closeReason);
			this.#sseConnection = null;
		}
		this.#rejectPending(closeReason);
		if (this.#tasks) {
			while (this.#tasks.size > 0) await Promise.allSettled(this.#tasks);
		}
		if (this.#pendingAdmissions > 0)
			logger.debug(
				"MCP legacy SSE local cleanup complete; callback drain unconfirmed; remote work not confirmed stopped",
			);
		const onClose = this.onClose;
		this.onClose = undefined;
		if (wasConnected) onClose?.();
	}
}

/** Create and connect a legacy HTTP+SSE transport. */
export async function createSseTransport(
	config: MCPSseServerConfig,
	signal?: AbortSignal,
	transportOptions?: MCPTransportOptions,
): Promise<LegacySseTransport> {
	const transport = new LegacySseTransport(config, transportOptions);
	try {
		await transport.connect(signal);
		signal?.throwIfAborted();
		transportOptions?.signal?.throwIfAborted();
		return transport;
	} catch (error) {
		await transport.close().catch(() => {});
		throw error;
	}
}
