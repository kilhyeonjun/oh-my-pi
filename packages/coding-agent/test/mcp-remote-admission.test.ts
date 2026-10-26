import { afterEach, describe, expect, it } from "bun:test";
import { withTimeout } from "@oh-my-pi/pi-utils";
import { MCPAdmissionError } from "../src/mcp/errors";
import { analyzeAuthError } from "../src/mcp/oauth-discovery";
import { createHttpTransport, getMCPHttpAuthError, HttpTransport } from "../src/mcp/transports/http";
import { createSseTransport, LegacySseTransport } from "../src/mcp/transports/sse";
import type { MCPBeforeSendEvent, MCPTransportOptions } from "../src/mcp/types";

const encoder = new TextEncoder();
const servers: Bun.Server<undefined>[] = [];
const transports: (HttpTransport | LegacySseTransport)[] = [];
afterEach(async () => {
	await Promise.allSettled(transports.splice(0).map(transport => bounded(transport.close())));
	for (const server of servers.splice(0)) server.stop(true);
});

function fixture(
	kind: "http" | "sse",
	behavior?: (request: Request, body: Record<string, unknown>) => Response | undefined,
) {
	const received: { method: string; path: string; body: string }[] = [];
	let stream: ReadableStreamDefaultController<Uint8Array> | undefined;
	const server = Bun.serve({
		port: 0,
		async fetch(request) {
			const url = new URL(request.url);
			const body = await request.text();
			received.push({ method: request.method, path: url.pathname, body });
			if (request.method === "GET") {
				return new Response(
					new ReadableStream<Uint8Array>({
						start(controller) {
							stream = controller;
							controller.enqueue(
								encoder.encode(kind === "sse" ? "event: endpoint\ndata: /messages\n\n" : ": connected\n\n"),
							);
						},
					}),
					{ headers: { "Content-Type": "text/event-stream" } },
				);
			}
			if (request.method === "DELETE") return new Response(null, { status: 204 });
			const message = JSON.parse(body) as Record<string, unknown>;
			const custom = behavior?.(request, message);
			if (custom) return custom;
			if (kind === "sse") {
				if (message.id !== undefined && message.method)
					stream!.enqueue(
						encoder.encode(
							`data: ${JSON.stringify({ jsonrpc: "2.0", id: message.id, result: { ok: true } })}\n\n`,
						),
					);
				return new Response(null, { status: 202 });
			}
			return Response.json({ jsonrpc: "2.0", id: message.id, result: { ok: true } });
		},
	});
	servers.push(server);
	return {
		url: `http://127.0.0.1:${server.port}/mcp`,
		received,
		emit(message: unknown) {
			stream!.enqueue(encoder.encode(`data: ${JSON.stringify(message)}\n\n`));
		},
	};
}

async function connect(kind: "http" | "sse", url: string, timeout: number, options: MCPTransportOptions) {
	const config = { type: kind, url, timeout, headerPolicy: "origin-locked" as const };
	const transport =
		kind === "http"
			? await createHttpTransport({ ...config, type: "http" }, options)
			: await createSseTransport({ ...config, type: "sse" }, undefined, options);
	transports.push(transport);
	return transport;
}

// A test watchdog is not a transport deadline: timeout:0 tests can only settle
// because the explicit caller/owner/close invalidation reaches native admission.
function bounded<T>(promise: Promise<T>): Promise<T> {
	return withTimeout(promise, 1_000, "transport-facing admission or close did not settle");
}

describe("guarded remote MCP physical effects", () => {
	for (const kind of ["http", "sse"] as const) {
		it(`${kind}: denial prevents the actual POST and exposes no callback diagnostics`, async () => {
			const remote = fixture(kind);
			const transport = await connect(kind, remote.url, 100, {
				beforeSend(event) {
					if (event.method === "POST") throw new Error("private callback payload");
				},
			});
			const request = transport.request("tools/call", { secret: "wire-value" });
			await expect(bounded(request)).rejects.toBeInstanceOf(MCPAdmissionError);
			await expect(request).rejects.not.toThrow("private callback payload");
			await expect(bounded(transport.notify("notifications/cancelled", { requestId: 1 }))).rejects.toBeInstanceOf(
				MCPAdmissionError,
			);
			expect(remote.received.filter(request => request.method === "POST")).toHaveLength(0);
		});

		it(`${kind}: never-settling admission respects the configured request deadline`, async () => {
			const remote = fixture(kind);
			const entered = Promise.withResolvers<void>();
			const transport = await connect(kind, remote.url, 35, {
				beforeSend(event) {
					if (event.method !== "POST") return;
					entered.resolve();
					return Promise.withResolvers<void>().promise;
				},
			});
			const request = transport.request("tools/call");
			await entered.promise;
			await expect(bounded(request)).rejects.toThrow(/timeout/i);
			await bounded(transport.close());
			expect(remote.received.filter(request => request.method === "POST")).toHaveLength(0);
		});

		it(`${kind}: explicit abort settles admission and a late allow sends nothing`, async () => {
			const remote = fixture(kind);
			const entered = Promise.withResolvers<void>();
			const allow = Promise.withResolvers<void>();
			const transport = await connect(kind, remote.url, 0, {
				beforeSend(event) {
					if (event.method === "POST") {
						entered.resolve();
						return allow.promise;
					}
				},
			});
			const controller = new AbortController();
			const request = transport.request("tools/call", {}, { signal: controller.signal });
			await entered.promise;
			controller.abort(new DOMException("caller canceled", "AbortError"));
			await expect(bounded(request)).rejects.toMatchObject({ name: "AbortError" });
			allow.resolve();
			await bounded(transport.close());
			expect(remote.received.filter(request => request.method === "POST")).toHaveLength(0);
		});

		it(`${kind}: disabled deadlines still permit bounded close and inert late rejection`, async () => {
			const remote = fixture(kind);
			const entered = Promise.withResolvers<void>();
			const gate = Promise.withResolvers<void>();
			const transport = await connect(kind, remote.url, 0, {
				beforeSend(event) {
					if (event.method === "POST") {
						entered.resolve();
						return gate.promise;
					}
				},
			});
			const request = transport.request("tools/call");
			void request.catch(() => {});
			await entered.promise;
			await bounded(transport.close());
			await expect(bounded(request)).rejects.toMatchObject({ name: "AbortError" });
			gate.reject(new Error("late private rejection"));
			await gate.promise.catch(() => {});
			expect(remote.received.filter(request => request.method === "POST")).toHaveLength(0);
		});

		it(`${kind}: auth replay is re-admitted before the second physical POST`, async () => {
			let posts = 0;
			const remote = fixture(kind, () => {
				posts++;
				return new Response("secret echo", { status: 401 });
			});
			let gates = 0;
			const transport = await connect(kind, remote.url, 200, {
				beforeSend(event) {
					if (event.method === "POST" && ++gates === 2) throw new Error("deny replay");
				},
			});
			transport.onAuthError = async () => ({ Authorization: "synthetic-token" });
			await expect(bounded(transport.request("tools/call"))).rejects.toBeInstanceOf(MCPAdmissionError);
			expect(posts).toBe(1);
			expect(gates).toBe(2);
		});

		it(`${kind}: denied successful handler response never creates a second error response`, async () => {
			const remote = fixture(kind);
			const denied = Promise.withResolvers<Error>();
			let responses = 0;
			const transport = await connect(kind, remote.url, 200, {
				beforeSend(event) {
					if (event.body && "result" in JSON.parse(event.body)) {
						responses++;
						throw new Error("deny response");
					}
				},
			});
			transport.onRequest = async () => ({ approvedHandler: true });
			transport.onError = denied.resolve;
			if (transport instanceof HttpTransport) await transport.startSSEListener();
			remote.emit({ jsonrpc: "2.0", id: "server-request", method: "sampling/createMessage", params: {} });
			expect(await bounded(denied.promise)).toBeInstanceOf(MCPAdmissionError);
			await bounded(transport.close());
			expect(responses).toBe(1);
			expect(remote.received.filter(request => request.method === "POST")).toHaveLength(0);
		});
	}

	it("rejects guarded unlocked remote connections before any network effect", async () => {
		const remote = fixture("http");
		await expect(createHttpTransport({ type: "http", url: remote.url }, { beforeSend() {} })).rejects.toBeInstanceOf(
			MCPAdmissionError,
		);
		await expect(
			createSseTransport({ type: "sse", url: remote.url }, undefined, { beforeSend() {} }),
		).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(remote.received).toHaveLength(0);
	});

	it("keeps the optional HTTP GET denial terminal rather than successful initialization", async () => {
		const remote = fixture("http");
		const transport = await connect("http", remote.url, 100, {
			beforeSend() {
				throw new Error("deny GET");
			},
		});
		await expect((transport as HttpTransport).startSSEListener()).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(remote.received).toHaveLength(0);
	});

	it("re-admits every allowed GET redirect and binds the immutable exact wire", async () => {
		const seen: MCPBeforeSendEvent[] = [];
		const fetched: string[] = [];
		const server = Bun.serve({
			port: 0,
			fetch(request) {
				fetched.push(new URL(request.url).pathname);
				return new URL(request.url).pathname === "/start"
					? new Response(null, { status: 302, headers: { Location: "/redirected" } })
					: new Response(null, { status: 405 });
			},
		});
		servers.push(server);
		const transport = await connect("http", `http://127.0.0.1:${server.port}/start`, 200, {
			beforeSend(event) {
				seen.push(event);
				expect(Object.isFrozen(event)).toBe(true);
				if (event.url?.endsWith("/redirected")) throw new Error("deny destination");
			},
		});
		await expect((transport as HttpTransport).startSSEListener()).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(fetched).toEqual(["/start"]);
		expect(seen.map(event => event.method)).toEqual(["GET", "GET"]);
		expect(seen[0].body).toBeUndefined();
		expect(Object.keys(seen[0])).not.toContain("headers");
	});

	it("owner revocation while a 307 hop waits prevents any late redirected POST", async () => {
		const fetched: string[] = [];
		const server = Bun.serve({
			port: 0,
			fetch(request) {
				fetched.push(new URL(request.url).pathname);
				return new Response(null, { status: 307, headers: { Location: "/second" } });
			},
		});
		servers.push(server);
		const owner = new AbortController();
		const entered = Promise.withResolvers<void>();
		const allow = Promise.withResolvers<void>();
		const events: MCPBeforeSendEvent[] = [];
		const transport = await connect("http", `http://127.0.0.1:${server.port}/first`, 0, {
			signal: owner.signal,
			beforeSend(event) {
				events.push(event);
				if (event.url?.endsWith("/second")) {
					entered.resolve();
					return allow.promise;
				}
			},
		});
		const request = transport.request("tools/call", { exact: "serialized" });
		await entered.promise;
		owner.abort(new DOMException("owner revoked", "AbortError"));
		await expect(bounded(request)).rejects.toMatchObject({ name: "AbortError" });
		allow.resolve();
		await bounded(transport.close());
		expect(fetched).toEqual(["/first"]);
		expect(events[1].body).toBe(events[0].body);
		expect(JSON.parse(events[0].body!).params).toEqual({ exact: "serialized" });
	});

	it("resume GET denial preserves terminal identity and never replays accepted POST", async () => {
		let posts = 0;
		let gets = 0;
		const server = Bun.serve({
			port: 0,
			async fetch(request) {
				if (request.method === "GET") {
					gets++;
					return new Response(null, { status: 500 });
				}
				posts++;
				await request.text();
				return new Response("id: accepted\nretry: 0\ndata: {}\n\n", {
					headers: { "Content-Type": "text/event-stream" },
				});
			},
		});
		servers.push(server);
		const transport = await connect("http", `http://127.0.0.1:${server.port}/mcp`, 200, {
			beforeSend(event) {
				if (event.method === "GET") throw new Error("deny resume");
			},
		});
		transport.onAuthError = async () => {
			throw new Error("must not refresh accepted POST");
		};
		await expect(bounded(transport.request("tools/call"))).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(posts).toBe(1);
		expect(gets).toBe(0);
	});

	it("DELETE denial propagates after local resources close and remote termination stays unconfirmed", async () => {
		const remote = fixture("http", (_request, message) =>
			Response.json(
				{ jsonrpc: "2.0", id: message.id, result: {} },
				{ headers: { "Mcp-Session-Id": "synthetic-session" } },
			),
		);
		let closed = false;
		const transport = await connect("http", remote.url, 100, {
			beforeSend(event) {
				if (event.method === "DELETE") throw new Error("deny cleanup");
			},
		});
		transport.onClose = () => {
			closed = true;
		};
		await transport.request("initialize");
		await expect(bounded(transport.close())).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(transport.connected).toBe(false);
		expect(closed).toBe(true);
		expect(remote.received.filter(request => request.method === "DELETE")).toHaveLength(0);
	});

	it("never-settling DELETE with timeout:0 cannot hang local close or send after late allow", async () => {
		const remote = fixture("http", (_request, message) =>
			Response.json(
				{ jsonrpc: "2.0", id: message.id, result: {} },
				{ headers: { "Mcp-Session-Id": "synthetic-session" } },
			),
		);
		const gate = Promise.withResolvers<void>();
		const transport = await connect("http", remote.url, 0, {
			beforeSend(event) {
				if (event.method === "DELETE") return gate.promise;
			},
		});
		await transport.request("initialize");
		await expect(bounded(transport.close())).rejects.toThrow(/termination unconfirmed/);
		gate.resolve();
		await gate.promise.catch(() => {});
		expect(transport.connected).toBe(false);
		expect(remote.received.filter(request => request.method === "DELETE")).toHaveLength(0);
	});

	it("legacy factory retains and closes an aborted never-settling startup GET", async () => {
		const remote = fixture("sse");
		const entered = Promise.withResolvers<void>();
		const allow = Promise.withResolvers<void>();
		const caller = new AbortController();
		const creation = createSseTransport(
			{ type: "sse", url: remote.url, timeout: 0, headerPolicy: "origin-locked" },
			caller.signal,
			{
				beforeSend() {
					entered.resolve();
					return allow.promise;
				},
			},
		);
		await entered.promise;
		caller.abort(new DOMException("creation abandoned", "AbortError"));
		await expect(bounded(creation)).rejects.toMatchObject({ name: "AbortError" });
		allow.resolve();
		await allow.promise;
		expect(remote.received).toHaveLength(0);
	});

	it("HTTP optional startup GET waiting without a deadline is canceled by close", async () => {
		const remote = fixture("http");
		const entered = Promise.withResolvers<void>();
		const allow = Promise.withResolvers<void>();
		const transport = await connect("http", remote.url, 0, {
			beforeSend() {
				entered.resolve();
				return allow.promise;
			},
		});
		const listener = (transport as HttpTransport).startSSEListener();
		void listener.catch(() => {});
		await entered.promise;
		await bounded(transport.close());
		await expect(bounded(listener)).rejects.toMatchObject({ name: "AbortError" });
		allow.resolve();
		await allow.promise;
		expect(remote.received).toHaveLength(0);
	});

	it("a successful legacy handshake outlives its cleared handshake timer", async () => {
		const remote = fixture("sse");
		const owner = new AbortController();
		const entered = Promise.withResolvers<void>();
		const transport = await connect("sse", remote.url, 25, {
			signal: owner.signal,
			beforeSend(event) {
				if (event.method === "POST") {
					entered.resolve();
					return Promise.withResolvers<void>().promise;
				}
			},
		});
		// The request deadline is native and independent of the completed handshake.
		const request = transport.request("tools/call");
		await entered.promise;
		await expect(bounded(request)).rejects.toThrow(/timeout/i);
		expect(transport.connected).toBe(true);
		await bounded(transport.close());
	});

	it("immutable serialized POST remains the admitted body despite mutation during the hook", async () => {
		const remote = fixture("http");
		const params = { exact: "before" };
		let admitted = "";
		const transport = await connect("http", remote.url, 100, {
			beforeSend(event) {
				admitted = event.body!;
				params.exact = "after";
			},
		});
		await transport.request("tools/call", params);
		expect(remote.received[0].body).toBe(admitted);
		expect(JSON.parse(admitted).params).toEqual({ exact: "before" });
	});

	it("guarded HTTP auth diagnostics redact echoed values while retaining private native discovery input", async () => {
		const remote = fixture(
			"http",
			() =>
				new Response("private server echo", {
					status: 401,
					headers: {
						"WWW-Authenticate": 'Bearer resource_metadata="https://synthetic.invalid/resource"',
						"Mcp-Auth-Server": "https://synthetic.invalid/auth",
					},
				}),
		);
		const transport = await connect("http", remote.url, 100, { beforeSend() {} });
		const failure = await transport.request("initialize").catch(error => error as Error);
		expect(failure).toBeInstanceOf(Error);
		if (!(failure instanceof Error)) throw new Error("Expected native transport failure");
		expect(failure.message).not.toContain("private server echo");
		expect(failure.message).not.toContain("synthetic.invalid");
		const auth = analyzeAuthError(getMCPHttpAuthError(failure), remote.url);
		expect(auth.requiresAuth).toBe(true);
		expect(auth.authServerUrl).toBe("https://synthetic.invalid/auth");
		expect(auth.resourceMetadataUrl).toBe("https://synthetic.invalid/resource");
	});
});
