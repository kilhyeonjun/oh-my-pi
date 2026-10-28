import { Database } from "bun:sqlite";
import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AuthStorage, SqliteAuthCredentialStore } from "@oh-my-pi/pi-ai";
import { withTimeout } from "@oh-my-pi/pi-utils";
import { callTool } from "../src/mcp/client";
import { formatMCPToolFailure, MCPTransportError } from "../src/mcp/errors";
import { MCPManager } from "../src/mcp/manager";
import { createHttpTransport } from "../src/mcp/transports/http";
import type { MCPTransportOptions } from "../src/mcp/types";

const cleanups: Array<() => void | Promise<void>> = [];
const encoder = new TextEncoder();
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

// Watchdog only; all deadline assertions depend on the native transport timer.
function bounded<T>(promise: Promise<T>): Promise<T> {
	return withTimeout(promise, 2_000, "HTTP admission lifetime failed to settle");
}

async function fixture(
	timeout: number,
	respond: (body: { id: number; method: string }, post: number) => Response | Promise<Response>,
	options: MCPTransportOptions = { beforeSend() {} },
) {
	let posts = 0;
	const server = Bun.serve({
		port: 0,
		async fetch(request) {
			if (request.method !== "POST") return new Response(null, { status: 204 });
			const body = (await request.json()) as { id: number; method: string };
			return respond(body, ++posts);
		},
	});
	cleanups.push(() => server.stop(true));
	const transport = await createHttpTransport(
		{ type: "http", url: `${server.url}mcp`, timeout, headerPolicy: "origin-locked" },
		options,
	);
	cleanups.push(() => bounded(transport.close()));
	return { transport, posts: () => posts };
}

function result(id: number): Response {
	return Response.json({ jsonrpc: "2.0", id, result: { ok: true } });
}

function streamed(id: number, error: unknown): Response {
	return new Response(`id: malformed-event\ndata: ${JSON.stringify({ jsonrpc: "2.0", id, error })}\n\n`, {
		headers: { "Content-Type": "text/event-stream" },
	});
}

// These loopback integrations deliberately exercise the native timer against
// real network I/O. Fake clocks cannot demonstrate platform fetch/body abort or
// post-result stream survival; sleeps below cross those explicit boundaries.
describe("guarded HTTP logical request budget", () => {
	it("bounds a nonsettling native auth hook and makes its late allow inert", async () => {
		const owner = new AbortController();
		const entered = Promise.withResolvers<void>();
		const allow = Promise.withResolvers<Record<string, string> | null>();
		// Leave setup headroom for real loopback I/O; the native deadline must
		// expire while auth is held, not before the fixture reaches the hook.
		const f = await fixture(1_000, () => new Response(null, { status: 401 }), {
			signal: owner.signal,
			beforeSend() {},
		});
		cleanups.push(() => {
			owner.abort();
			allow.resolve(null);
		});
		let effectiveSignal: AbortSignal | undefined;
		f.transport.onAuthError = signal => {
			effectiveSignal = signal;
			entered.resolve();
			return allow.promise;
		};
		const request = f.transport.request("tools/call");
		void request.catch(() => {});
		await bounded(entered.promise);
		await expect(bounded(request)).rejects.toMatchObject({ failure: "timeout" });
		expect(owner.signal.aborted).toBe(false);
		expect(effectiveSignal?.aborted).toBe(true);
		allow.resolve({ Authorization: "synthetic-late" });
		await allow.promise;
		await bounded(f.transport.close());
		expect(f.posts()).toBe(1);
	});

	it("does not reset the budget for an authorized retry gate", async () => {
		// Real socket/auth replay deliberately straddles the platform deadline;
		// Bun's fake clock can defer socket publication (DEVELOPMENT.md).
		const entered = Promise.withResolvers<void>();
		const allow = Promise.withResolvers<void>();
		let gates = 0;
		const f = await fixture(
			1_000,
			(body, post) => (post === 1 ? new Response(null, { status: 401 }) : result(body.id)),
			{
				beforeSend(event) {
					if (event.method !== "POST" || ++gates !== 2) return;
					entered.resolve();
					return allow.promise;
				},
			},
		);
		f.transport.onAuthError = async () => {
			await Bun.sleep(650);
			return { Authorization: "synthetic-fresh" };
		};
		const request = f.transport.request("tools/call");
		void request.catch(() => {});
		await bounded(entered.promise);
		// This allow is after the original budget but before a reset retry budget.
		const lateAllow = Bun.sleep(650).then(() => allow.resolve());
		await expect(bounded(request)).rejects.toMatchObject({ failure: "timeout" });
		await lateAllow;
		await bounded(f.transport.close());
		expect(f.posts()).toBe(1);
	});

	it("keeps caller cancellation distinct from timeout during auth", async () => {
		const entered = Promise.withResolvers<void>();
		const caller = new AbortController();
		const f = await fixture(500, () => new Response(null, { status: 401 }));
		f.transport.onAuthError = () => {
			entered.resolve();
			return Promise.withResolvers<Record<string, string> | null>().promise;
		};
		const request = f.transport.request("tools/call", {}, { signal: caller.signal });
		void request.catch(() => {});
		await bounded(entered.promise);
		caller.abort(new DOMException("caller canceled", "AbortError"));
		await expect(bounded(request)).rejects.toMatchObject({ name: "AbortError" });
		expect(f.posts()).toBe(1);
	});

	it("timeout zero permits auth completion and a real second POST", async () => {
		const entered = Promise.withResolvers<void>();
		const allow = Promise.withResolvers<Record<string, string> | null>();
		const f = await fixture(0, (body, post) => (post === 1 ? new Response(null, { status: 401 }) : result(body.id)));
		f.transport.onAuthError = () => {
			entered.resolve();
			return allow.promise;
		};
		let settled = false;
		const request = f.transport.request("tools/call").finally(() => {
			settled = true;
		});
		await bounded(entered.promise);
		await Bun.sleep(80);
		expect(settled).toBe(false);
		allow.resolve({ Authorization: "synthetic-fresh" });
		expect(await bounded(request)).toEqual({ ok: true });
		expect(f.posts()).toBe(2);
	});

	it("detaches the caller and deadline after an SSE result while retaining background drain ownership", async () => {
		let stream: ReadableStreamDefaultController<Uint8Array> | undefined;
		const f = await fixture(
			60,
			body =>
				new Response(
					new ReadableStream<Uint8Array>({
						start(controller) {
							stream = controller;
							controller.enqueue(
								encoder.encode(
									`data: ${JSON.stringify({ jsonrpc: "2.0", id: body.id, result: { ok: true } })}\n\n`,
								),
							);
						},
					}),
					{ headers: { "Content-Type": "text/event-stream" } },
				),
		);
		const caller = new AbortController();
		const notification = Promise.withResolvers<unknown>();
		f.transport.onNotification = (_method, params) => notification.resolve(params);
		expect(await bounded(f.transport.request<{ ok: boolean }>("tools/call", {}, { signal: caller.signal }))).toEqual({
			ok: true,
		});
		caller.abort();
		await Bun.sleep(90);
		stream!.enqueue(
			encoder.encode('data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"drained":true}}\n\n'),
		);
		expect(await bounded(notification.promise)).toEqual({ drained: true });
		stream!.close();
		await bounded(f.transport.close());
	});

	it("threads the request deadline through the real manager auth-refresh preflight", async () => {
		const root = await mkdtemp(join(tmpdir(), "mcp-http-lifetime-"));
		cleanups.push(() => rm(root, { recursive: true, force: true }));
		const auth = new AuthStorage(new SqliteAuthCredentialStore(new Database(":memory:")));
		await auth.credentials.reload();
		cleanups.push(() => auth.close());
		await auth.credentials.set("lifetime-fixture", {
			type: "oauth",
			access: "synthetic-current",
			refresh: "synthetic-refresh",
			expires: Date.now() + 3600_000,
		});
		let toolPosts = 0;
		let tokenPosts = 0;
		const server = Bun.serve({
			port: 0,
			async fetch(request) {
				if (new URL(request.url).pathname === "/token") {
					tokenPosts++;
					return Response.json({ access_token: "synthetic-rotated", token_type: "Bearer", expires_in: 3600 });
				}
				if (request.method !== "POST") return new Response(null, { status: 405 });
				const body = (await request.json()) as { id?: number; method: string };
				if (body.method === "tools/call") {
					toolPosts++;
					return new Response(null, { status: 401 });
				}
				if (body.id === undefined) return new Response(null, { status: 202 });
				return Response.json({
					jsonrpc: "2.0",
					id: body.id,
					result:
						body.method === "initialize"
							? {
									protocolVersion: "2025-03-26",
									serverInfo: { name: "fixture", version: "1" },
									capabilities: {},
								}
							: {},
				});
			},
		});
		cleanups.push(() => server.stop(true));
		const owner = new AbortController();
		const entered = Promise.withResolvers<void>();
		const allow = Promise.withResolvers<void>();
		const manager = new MCPManager(root, null, undefined, undefined, () => ({
			signal: owner.signal,
			transportOptions: { beforeSend() {} },
			beforeAuth(event) {
				if (event.phase !== "auth-refresh") return;
				entered.resolve();
				return allow.promise;
			},
		}));
		manager.setAuthStorage(auth);
		cleanups.push(() => manager.disconnectAll());
		await manager.connectServers(
			{
				fixture: {
					type: "http",
					url: `${server.url}mcp`,
					timeout: 100,
					auth: { type: "oauth", credentialId: "lifetime-fixture", tokenUrl: `${server.url}token` },
				},
			},
			{},
			undefined,
			0,
		);
		const connection = await manager.waitForConnection("fixture");
		const request = callTool(connection, "read_fixture");
		void request.catch(() => {});
		await bounded(entered.promise);
		await expect(bounded(request)).rejects.toMatchObject({ failure: "timeout" });
		expect(owner.signal.aborted).toBe(false);
		expect(toolPosts).toBe(1);
		expect(tokenPosts).toBe(0);
		allow.resolve();
		await allow.promise;
		// Let the preflight continuation run before cleanup revokes its binding.
		await Bun.sleep(20);
		expect(toolPosts).toBe(1);
		expect(tokenPosts).toBe(0);
	});
});

describe("guarded HTTP SSE error boundary", () => {
	for (const code of ["OPAQUE_SYNTHETIC_CREDENTIAL_9zQ", null, {}, 1.25, Number.MAX_SAFE_INTEGER + 1]) {
		it(`rejects malformed remote code ${JSON.stringify(code)} without retaining it`, async () => {
			const f = await fixture(100, body =>
				streamed(body.id, { code, message: "PRIVATE_REMOTE_BODY", data: "PRIVATE_REMOTE_DATA" }),
			);
			const error = await bounded(f.transport.request("tools/call")).catch(error => error);
			expect(error).toBeInstanceOf(MCPTransportError);
			if (!(error instanceof MCPTransportError)) throw error;
			expect(error.failure).toBe("malformed_response");
			expect(error.code).toBeUndefined();
			expect(error.cause).toBeUndefined();
			const diagnostics = [
				String(error),
				JSON.stringify(error),
				formatMCPToolFailure(error, "fixture", "read"),
			].join("\n");
			expect(diagnostics).not.toContain("OPAQUE_SYNTHETIC_CREDENTIAL_9zQ");
			expect(diagnostics).not.toContain("PRIVATE_REMOTE_BODY");
			expect(diagnostics).not.toContain("PRIVATE_REMOTE_DATA");
		});
	}

	it("preserves valid numeric JSON-RPC errors while redacting their remote text", async () => {
		const f = await fixture(100, body => streamed(body.id, { code: -32602, message: "PRIVATE_REMOTE_BODY" }));
		const error = await bounded(f.transport.request("tools/call")).catch(error => error);
		expect(error).toBeInstanceOf(MCPTransportError);
		if (!(error instanceof MCPTransportError)) throw error;
		expect(error.code).toBe(-32602);
		expect(error.failure).toBe("json_rpc");
		expect(formatMCPToolFailure(error, "fixture", "read")).not.toContain("PRIVATE_REMOTE_BODY");
	});
});
