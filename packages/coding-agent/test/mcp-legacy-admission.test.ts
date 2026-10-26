import { Database } from "bun:sqlite";
import { afterEach, describe, expect, it, vi } from "bun:test";
import { AuthStorage, SqliteAuthCredentialStore } from "@oh-my-pi/pi-ai";
import { withTimeout } from "@oh-my-pi/pi-utils";
import { formatMCPToolFailure, MCPAdmissionError, MCPTransportError } from "../src/mcp/errors";
import { MCPManager } from "../src/mcp/manager";
import { createSseTransport, LegacySseTransport } from "../src/mcp/transports/sse";
import type { MCPTransportOptions } from "../src/mcp/types";

const encoder = new TextEncoder();
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

function bounded<T>(promise: Promise<T>): Promise<T> {
	return withTimeout(promise, 1_000, "legacy admission operation did not settle");
}

function fixture() {
	const posts: Record<string, unknown>[] = [];
	let stream: ReadableStreamDefaultController<Uint8Array> | undefined;
	let unauthorized = false;
	let tokenRequests = 0;
	let responseError: unknown;
	let returnError = false;
	const emit = (message: unknown) => stream!.enqueue(encoder.encode(`data: ${JSON.stringify(message)}\n\n`));
	const server = Bun.serve({
		port: 0,
		async fetch(request) {
			const pathname = new URL(request.url).pathname;
			if (pathname === "/token") {
				tokenRequests++;
				return Response.json({ access_token: "synthetic-unused", token_type: "Bearer", expires_in: 3600 });
			}
			if (request.method === "GET") {
				return new Response(
					new ReadableStream<Uint8Array>({
						start(controller) {
							stream = controller;
							controller.enqueue(encoder.encode("event: endpoint\ndata: /messages\n\n"));
						},
					}),
					{ headers: { "Content-Type": "text/event-stream" } },
				);
			}
			const message = (await request.json()) as Record<string, unknown>;
			posts.push(message);
			if (unauthorized) return new Response(null, { status: 401 });
			if (message.id !== undefined && typeof message.method === "string") {
				const result =
					message.method === "initialize"
						? {
								protocolVersion: "2024-11-05",
								serverInfo: { name: "legacy-admission-fixture", version: "1" },
								capabilities: { tools: {} },
							}
						: message.method === "tools/list"
							? { tools: [] }
							: { ok: true };
				emit(
					returnError
						? { jsonrpc: "2.0", id: message.id, error: responseError }
						: { jsonrpc: "2.0", id: message.id, result },
				);
			}
			return new Response(null, { status: 202 });
		},
	});
	cleanups.push(async () => server.stop(true));
	return {
		url: `${server.url}mcp`,
		tokenUrl: `${server.url}token`,
		posts,
		emit,
		denyPosts() {
			unauthorized = true;
		},
		error(value: unknown) {
			returnError = true;
			responseError = value;
		},
		tokenRequests: () => tokenRequests,
	};
}

async function connect(url: string, timeout: number, options?: MCPTransportOptions) {
	const transport = await createSseTransport(
		{ type: "sse", url, timeout, headerPolicy: "origin-locked" },
		undefined,
		options,
	);
	cleanups.push(() => bounded(transport.close()));
	return transport;
}

describe("legacy SSE admitted response correlation", () => {
	for (const cancellation of ["deadline", "caller"] as const) {
		for (const late of ["allow", "deny"] as const) {
			it(`ignores a matching unadmitted reply through ${cancellation} and late ${late}`, async () => {
				const remote = fixture();
				const entered = Promise.withResolvers<void>();
				const gate = Promise.withResolvers<void>();
				const transport = await connect(remote.url, cancellation === "deadline" ? 80 : 0, {
					beforeSend(event) {
						if (event.method !== "POST") return;
						entered.resolve();
						return gate.promise;
					},
				});
				const controller = new AbortController();
				const request = transport.request("tools/call", {}, { signal: controller.signal });
				let settled = false;
				void request.then(
					() => {
						settled = true;
					},
					() => {
						settled = true;
					},
				);
				await entered.promise;
				const dispatched = Promise.withResolvers<void>();
				transport.onNotification = method => {
					if (method === "fixture/dispatch-barrier") dispatched.resolve();
				};
				remote.emit({ jsonrpc: "2.0", id: 1, result: { unadmitted: true } });
				remote.emit({ jsonrpc: "2.0", method: "fixture/dispatch-barrier" });
				await bounded(dispatched.promise);
				expect(settled).toBe(false);
				expect(remote.posts).toHaveLength(0);
				if (cancellation === "caller") controller.abort(new DOMException("fixture canceled", "AbortError"));
				if (cancellation === "deadline") await expect(bounded(request)).rejects.toThrow(/timeout/i);
				else await expect(bounded(request)).rejects.toMatchObject({ name: "AbortError" });
				if (late === "allow") gate.resolve();
				else gate.reject(new Error("synthetic late denial"));
				await gate.promise.catch(() => {});
				await bounded(transport.close());
				expect(remote.posts).toHaveLength(0);
			});
		}
	}

	it("accepts the response after the actual admitted POST", async () => {
		const remote = fixture();
		const transport = await connect(remote.url, 250, { beforeSend() {} });
		await expect(bounded(transport.request("tools/call"))).resolves.toEqual({ ok: true });
		expect(remote.posts).toHaveLength(1);
	});

	it("keeps the no-hook request behavior", async () => {
		const remote = fixture();
		const transport = await connect(remote.url, 250);
		await expect(bounded(transport.request("tools/call"))).resolves.toEqual({ ok: true });
		expect(remote.posts).toHaveLength(1);
	});
});

describe("legacy SSE guarded remote error diagnostics", () => {
	const secret = "SYNTHETIC_OPAQUE_SECRET_7d62";
	for (const [index, malformed] of [
		{ code: secret, message: secret, data: secret },
		secret,
		null,
		{ code: { nested: secret }, message: secret },
		{ code: -32001, message: { nested: secret } },
	].entries()) {
		it(`rejects malformed error shape ${index} without retaining remote fields`, async () => {
			const remote = fixture();
			remote.error(malformed);
			const transport = await connect(remote.url, 250, { beforeSend() {} });
			const error = await bounded(transport.request("tools/call")).catch(error => error);
			expect(error).toBeInstanceOf(MCPTransportError);
			if (!(error instanceof MCPTransportError)) throw error;
			expect(error.failure).toBe("malformed_response");
			const exposed = [
				String(error),
				JSON.stringify(error),
				Bun.inspect(error),
				formatMCPToolFailure(error, "fixture", "read"),
			].join("\n");
			expect(exposed).not.toContain(secret);
			expect(error.cause).toBeUndefined();
			expect(error.code).toBeUndefined();
		});
	}

	for (const code of [-32601, -32001]) {
		it(`retains the valid numeric server error ${code} but not its message or data`, async () => {
			const remote = fixture();
			remote.error({ code, message: secret, data: secret });
			const transport = await connect(remote.url, 250, { beforeSend() {} });
			const error = await bounded(transport.request("tools/call")).catch(error => error);
			expect(error).toBeInstanceOf(MCPTransportError);
			if (!(error instanceof MCPTransportError)) throw error;
			expect(error.failure).toBe("json_rpc");
			expect(error.code).toBe(code);
			expect(
				[
					String(error),
					JSON.stringify(error),
					Bun.inspect(error),
					formatMCPToolFailure(error, "fixture", "read"),
				].join("\n"),
			).not.toContain(secret);
			expect(error.cause).toBeUndefined();
		});
	}
});

describe("manager-owned legacy SSE auth-refresh denial", () => {
	for (const outgoing of ["notification", "server-response"] as const) {
		it(`${outgoing} preserves the exact native denial without replay or recovery`, async () => {
			const remote = fixture();
			const auth = new AuthStorage(new SqliteAuthCredentialStore(new Database(":memory:")));
			await auth.credentials.reload();
			await auth.credentials.set("legacy-admission-fixture", {
				type: "oauth",
				access: "synthetic-fixture-access",
				refresh: "synthetic-fixture-refresh",
				expires: Date.now() + 3_600_000,
			});
			cleanups.push(async () => auth.close());
			const owner = new AbortController();
			const phases: string[] = [];
			const statuses: string[] = [];
			let bindings = 0;
			let refreshSignal: AbortSignal | undefined;
			const manager = new MCPManager(
				"/tmp",
				null,
				undefined,
				{ ladderMs: [1, 1], retryBaseMs: 1, retryMaxMs: 2 },
				() => {
					bindings++;
					return {
						signal: owner.signal,
						transportOptions: { beforeSend() {} },
						beforeAuth(event) {
							phases.push(event.phase);
							if (event.phase === "auth-refresh") {
								refreshSignal = event.signal;
								throw new Error("synthetic private preflight denial");
							}
						},
					};
				},
			);
			manager.setAuthStorage(auth);
			manager.addConnectionStatusListener(event => statuses.push(event.type));
			cleanups.push(() => bounded(manager.disconnectAll()));
			await bounded(
				manager.connectServers(
					{
						fixture: {
							type: "sse",
							url: remote.url,
							timeout: 500,
							auth: { type: "oauth", credentialId: "legacy-admission-fixture", tokenUrl: remote.tokenUrl },
						},
					},
					{},
					undefined,
					0,
				),
			);
			const transport = manager.getConnection("fixture")?.transport;
			expect(transport).toBeInstanceOf(LegacySseTransport);
			if (!(transport instanceof LegacySseTransport)) throw new Error("fixture legacy connection missing");
			const before = remote.posts.length;
			const statusBefore = statuses.length;
			remote.denyPosts();
			const failure = Promise.withResolvers<unknown>();
			transport.onError = error => failure.resolve(error);
			let error: unknown;
			if (outgoing === "notification") {
				error = await bounded(transport.notify("notifications/fixture")).catch(error => error);
			} else {
				transport.onRequest = async () => ({ accepted: true });
				remote.emit({ jsonrpc: "2.0", id: "server-fixture", method: "fixture/read" });
				error = await bounded(failure.promise);
			}
			expect(error).toBeInstanceOf(MCPAdmissionError);
			expect(refreshSignal?.aborted).toBe(true);
			expect(error).toBe(refreshSignal?.reason);
			await expect(manager.reconnectServer("fixture")).rejects.toBe(error);
			await bounded(transport.close());
			vi.useFakeTimers();
			try {
				vi.advanceTimersByTime(15);
				await Promise.resolve();
			} finally {
				vi.useRealTimers();
			}
			expect(remote.posts).toHaveLength(before + 1);
			expect(remote.tokenRequests()).toBe(0);
			expect(bindings).toBe(1);
			expect(phases.filter(phase => phase === "auth-refresh")).toHaveLength(1);
			expect(statuses.slice(statusBefore)).not.toContain("reconnecting");
			const sent = remote.posts[before]!;
			if (outgoing === "notification") expect(sent.method).toBe("notifications/fixture");
			else {
				expect(sent.id).toBe("server-fixture");
				expect(sent.result).toEqual({ accepted: true });
				expect(sent.error).toBeUndefined();
			}
		});
	}
});
