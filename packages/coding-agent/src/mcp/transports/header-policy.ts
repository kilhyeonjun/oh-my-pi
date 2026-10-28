/**
 * HTTP header precedence and redirect-origin policy for remote MCP transports.
 *
 * Two invariants, applied at every transport fetch:
 *
 * 1. Client-generated headers (protocol headers like `Content-Type`, `Accept`,
 *    `Mcp-Session-Id`, and authorization) take precedence over configured
 *    headers with the same case-insensitive name. Configured headers can never
 *    corrupt the MCP wire protocol via casing tricks.
 * 2. Origin-locked servers (Agent Plugins §7.2.1) never forward configured
 *    headers to a different origin: redirects are followed manually and
 *    configured headers are attached only when the hop targets the configured
 *    origin. Method-changing redirects of non-GET requests are refused.
 */

import { runMCPAdmission } from "../admission";
import { checkMCPSendCustody, type MCPSendCustody } from "../invocation";
import { MCPAdmissionError } from "../errors";
import { getNeverAbortSignal } from "../timeout";
import type { MCPTransportOptions } from "../types";

/** Header buckets for one MCP HTTP request. */
interface MCPHeaderSources {
	/** Client-generated HTTP/MCP/authorization headers; win case-insensitively. */
	generated: Record<string, string>;
	/** Configured headers from the server entry (package or user config). */
	configured?: Record<string, string>;
}

/**
 * Merge configured headers under client-generated ones: a configured entry is
 * dropped when a generated header with the same case-insensitive name exists.
 */
export function mergeMCPHeaders({ generated, configured }: MCPHeaderSources): Record<string, string> {
	if (!configured) return { ...generated };
	const generatedNames = new Set<string>();
	for (const name in generated) generatedNames.add(name.toLowerCase());
	const merged: Record<string, string> = {};
	for (const name in configured) {
		if (!generatedNames.has(name.toLowerCase())) merged[name] = configured[name];
	}
	return { ...merged, ...generated };
}

/**
 * Set a client-generated header, removing any existing entry with the same
 * case-insensitive name so the generated value is the only one sent.
 */
export function setGeneratedHeader(headers: Record<string, string>, name: string, value: string): void {
	const lower = name.toLowerCase();
	for (const existing in headers) {
		if (existing.toLowerCase() === lower) delete headers[existing];
	}
	headers[name] = value;
}

/**
 * Return `headers` without any entry whose name case-insensitively matches
 * `name`. Used to keep transport-reserved protocol headers (e.g.
 * `MCP-Protocol-Version`) out of user-configured headers so config can never
 * inject them. Returns the original reference when there is nothing to strip,
 * so the common (no-match) path allocates nothing.
 */
export function withoutHeader(
	headers: Record<string, string> | undefined,
	name: string,
): Record<string, string> | undefined {
	if (!headers) return headers;
	const lower = name.toLowerCase();
	let hasMatch = false;
	for (const key in headers) {
		if (key.toLowerCase() === lower) {
			hasMatch = true;
			break;
		}
	}
	if (!hasMatch) return headers;
	const result: Record<string, string> = {};
	for (const key in headers) {
		if (key.toLowerCase() !== lower) result[key] = headers[key];
	}
	return result;
}

const REDIRECT_STATUSES: Record<number, true> = { 301: true, 302: true, 303: true, 307: true, 308: true };
const MAX_REDIRECT_HOPS = 5;
const FORBIDDEN_AUTH_HEADERS: Record<string, true> = {
	host: true,
	"content-length": true,
	"transfer-encoding": true,
	connection: true,
	upgrade: true,
	expect: true,
	accept: true,
	"content-type": true,
	"last-event-id": true,
	"mcp-session-id": true,
	"mcp-protocol-version": true,
	trailer: true,
	te: true,
};

/** Snapshot host output without invoking accessors or retaining private errors. */
function snapshotAuthHeaders(value: unknown): Readonly<Record<string, string>> {
	try {
		if (typeof value !== "object" || value === null || Array.isArray(value)) throw new MCPAdmissionError();
		const headers: Record<string, string> = {};
		const names: Record<string, true> = {};
		for (const key of Reflect.ownKeys(value)) {
			if (typeof key !== "string") throw new MCPAdmissionError();
			const descriptor = Object.getOwnPropertyDescriptor(value, key);
			if (!descriptor || !("value" in descriptor)) throw new MCPAdmissionError();
			if (!descriptor.enumerable) continue;
			const header: unknown = descriptor.value;
			const lower = key.toLowerCase();
			if (
				typeof header !== "string" ||
				!/^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(key) ||
				FORBIDDEN_AUTH_HEADERS[lower] ||
				lower.startsWith("proxy-") ||
				lower.startsWith("mcp-") ||
				names[lower]
			)
				throw new MCPAdmissionError();
			new Headers([[key, header]]);
			names[lower] = true;
			Object.defineProperty(headers, key, { value: header, enumerable: true });
		}
		return Object.freeze(headers);
	} catch {
		throw new MCPAdmissionError();
	}
}

export interface MCPFetchInit {
	method: "GET" | "POST" | "DELETE";
	body?: string;
	signal?: AbortSignal;
}

/**
 * Fetch an MCP endpoint with header precedence and, for origin-locked servers,
 * manual redirect handling that strips configured headers on cross-origin hops.
 *
 * Non-locked servers keep the platform default redirect behavior. Locked
 * non-GET requests only follow 307/308 (method-preserving); a 301/302/303
 * redirect of a JSON-RPC POST is a connection error, never a silent GET.
 *
 * `timeout: false` keeps the runtime's socket idle timer out of the MCP timeout
 * surface. MCP calls are long-poll shaped — a server may stay silent for as
 * long as the work takes — and that timer would end such a wait even where the
 * operator disabled MCP deadlines outright (`timeout: 0`,
 * `OMP_MCP_TIMEOUT_MS=0`). Deadlines and cancellation stay with `init.signal`,
 * which each transport composes from the configured per-request deadline,
 * caller cancellation, and transport close.
 */
export async function mcpFetch(
	url: string,
	init: MCPFetchInit,
	sources: MCPHeaderSources,
	originLocked: boolean,
	options?: MCPTransportOptions,
	transport: "http" | "sse" = "http",
	onCallback?: (pending: boolean) => void,
	onSend?: () => void,
	custody?: MCPSendCustody,
	onAuthResolve?: (pending: Promise<unknown>) => void,
): Promise<Response> {
	const beforeSend = options?.beforeSend;
	const resolveAuthHeaders = options?.resolveAuthHeaders;
	if (resolveAuthHeaders && (transport !== "http" || !originLocked || !beforeSend)) throw new MCPAdmissionError();
	const authSources = resolveAuthHeaders
		? { generated: { ...sources.generated }, configured: sources.configured ? { ...sources.configured } : undefined }
		: undefined;
	if (beforeSend && !originLocked) throw new MCPAdmissionError();
	// Only guarded sends snapshot the wire fields; strings are already serialized.
	if (beforeSend) init = Object.freeze({ method: init.method, body: init.body, signal: init.signal });
	if (!originLocked) {
		if (onSend) {
			init.signal?.throwIfAborted();
			onSend();
		}
		return fetch(url, { ...init, headers: mergeMCPHeaders(sources), timeout: false });
	}

	const configuredOrigin = new URL(url).origin;
	let currentUrl = url;
	for (let hop = 0; hop <= MAX_REDIRECT_HOPS; hop++) {
		const attachConfigured = new URL(currentUrl).origin === configuredOrigin;
		let headers = mergeMCPHeaders(attachConfigured ? sources : { generated: sources.generated });
		if (beforeSend) {
			const signal = init.signal ?? getNeverAbortSignal();
			if (custody) checkMCPSendCustody(custody, currentUrl, init.body);
			const event = Object.freeze({
				transport,
				url: currentUrl,
				method: init.method,
				body: init.body,
				signal,
				...(custody ? { association: custody.association, wireId: custody.wireId } : undefined),
				...(resolveAuthHeaders ? { admissionPhase: "prepare" as const } : undefined),
			});
			await runMCPAdmission(signal, async () => {
				onCallback?.(true);
				try {
					return await beforeSend(event);
				} finally {
					onCallback?.(false);
				}
			});
			signal.throwIfAborted();
			if (resolveAuthHeaders) {
				if (custody) checkMCPSendCustody(custody, currentUrl, init.body);
				const resolverEvent = Object.freeze({
					transport: "http" as const,
					url: currentUrl,
					method: init.method,
					signal,
					...(custody ? { association: custody.association } : undefined),
				});
				const pending = Promise.resolve()
					.then(() => {
						signal.throwIfAborted();
						return resolveAuthHeaders(resolverEvent);
					})
					.then(snapshotAuthHeaders, () => {
						throw new MCPAdmissionError();
					});
				onAuthResolve?.(pending);
				let resolved: Readonly<Record<string, string>> | undefined;
				await runMCPAdmission(signal, async () => {
					resolved = await pending;
				});
				signal.throwIfAborted();
				if (custody) checkMCPSendCustody(custody, currentUrl, init.body);
				const configured = { ...authSources!.configured };
				for (const [name, value] of Object.entries(resolved ?? {})) setGeneratedHeader(configured, name, value);
				headers = mergeMCPHeaders({ generated: authSources!.generated, configured });
				const finalEvent = Object.freeze({ ...event, admissionPhase: "final" as const });
				await runMCPAdmission(signal, async () => {
					onCallback?.(true);
					try {
						return await beforeSend(finalEvent);
					} finally {
						onCallback?.(false);
					}
				});
				signal.throwIfAborted();
			}
		}
		if (onSend) {
			if (!beforeSend) init.signal?.throwIfAborted();
			onSend();
		}
		if (custody) checkMCPSendCustody(custody, currentUrl, init.body);
		const response = await fetch(currentUrl, { ...init, headers, redirect: "manual", timeout: false });
		if (!REDIRECT_STATUSES[response.status]) return response;
		if (resolveAuthHeaders) {
			await response.body?.cancel();
			throw new MCPAdmissionError();
		}

		const location = response.headers.get("Location");
		if (!location) return response;
		await response.body?.cancel();
		if (init.method !== "GET" && response.status !== 307 && response.status !== 308) {
			throw new Error(`HTTP ${response.status}: server redirected a ${init.method} request; refusing to follow`);
		}
		currentUrl = new URL(location, currentUrl).href;
	}
	throw new Error(
		beforeSend ? "Too many MCP redirects" : `Too many redirects (> ${MAX_REDIRECT_HOPS}) fetching ${url}`,
	);
}
