import { afterEach, describe, expect, test } from "bun:test";
import { isRecord } from "@oh-my-pi/pi-utils";
import { MCPAdmissionError } from "../src/mcp/errors";
import { HttpTransport } from "../src/mcp/transports/http";
import { LegacySseTransport } from "../src/mcp/transports/sse";
import { StdioTransport } from "../src/mcp/transports/stdio";
import type { MCPBeforeSendEvent, MCPResolveAuthHeadersEvent, MCPTransportOptions } from "../src/mcp/types";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
const canary = "synthetic-transport-only-credential-canary";

async function httpFixture(options: MCPTransportOptions, redirect = false, responseStatus = 200) {
	const received: Array<{ headers: Headers; body: string }> = [];
	const server = Bun.serve({
		port: 0,
		async fetch(request) {
			const body = await request.text();
			received.push({ headers: request.headers, body });
			if (redirect) return new Response(null, { status: 307, headers: { Location: "/another-path" } });
			if (responseStatus !== 200) return new Response(null, { status: responseStatus });
			const parsed: unknown = JSON.parse(body);
			if (!isRecord(parsed)) return new Response(null, { status: 400 });
			return Response.json({ jsonrpc: "2.0", id: parsed.id, result: { content: [{ type: "text", text: "ok" }] } });
		},
	});
	const transport = new HttpTransport(
		{
			type: "http",
			url: `${server.url}mcp`,
			headerPolicy: "origin-locked",
			timeout: 1000,
			headers: { Authorization: "replaced-placeholder" },
		},
		options,
	);
	cleanups.push(async () => {
		try {
			await transport.close();
		} finally {
			server.stop(true);
		}
	});
	await transport.connect();
	return { transport, received };
}

describe("transport-only MCP credential references", () => {
	test("admission without a credential resolver remains a single unphased callback", async () => {
		const events: MCPBeforeSendEvent[] = [];
		const f = await httpFixture({
			beforeSend(event) {
				events.push(event);
			},
		});
		await f.transport.request("ping");
		expect(f.received).toHaveLength(1);
		expect(events).toHaveLength(1);
		expect(Object.hasOwn(events[0], "admissionPhase")).toBe(false);
		expect(f.received[0].headers.get("Authorization")).toBe("replaced-placeholder");
	});

	test("prepare denial performs no credential lookup or HTTP effect", async () => {
		let resolutions = 0;
		const f = await httpFixture({
			beforeSend() {
				throw new Error("denied");
			},
			resolveAuthHeaders() {
				resolutions++;
				return { Authorization: canary };
			},
		});
		await expect(f.transport.request("tools/call", { name: "send", arguments: {} })).rejects.toBeInstanceOf(
			MCPAdmissionError,
		);
		expect(resolutions).toBe(0);
		expect(f.received).toEqual([]);
	});

	test("policy changed while resolving is rechecked without a second once decision or any fetch", async () => {
		const gate = Promise.withResolvers<Readonly<Record<string, string>>>();
		const entered = Promise.withResolvers<void>();
		let allowed = true;
		let humanDecisions = 0;
		const phases: string[] = [];
		const f = await httpFixture({
			beforeSend(event) {
				phases.push(event.admissionPhase ?? "unphased");
				if (event.admissionPhase === "prepare") humanDecisions++;
				if (!allowed) throw new Error("policy changed");
			},
			resolveAuthHeaders() {
				entered.resolve();
				return gate.promise;
			},
		});
		const call = f.transport.request("tools/call", { name: "send", arguments: { text: "hello" } });
		const failure = call.catch((error: unknown) => error);
		cleanups.push(async () => gate.resolve({ Authorization: canary }));
		await entered.promise;
		allowed = false;
		gate.resolve({ Authorization: canary });
		expect(await failure).toBeInstanceOf(MCPAdmissionError);
		expect(phases).toEqual(["prepare", "final"]);
		expect(humanDecisions).toBe(1);
		expect(f.received).toEqual([]);
	});

	test("credential canary exists only in final HTTP headers, never callback bodies or resolver metadata", async () => {
		const events: MCPBeforeSendEvent[] = [];
		const resolverEvents: MCPResolveAuthHeadersEvent[] = [];
		const f = await httpFixture({
			beforeSend(event) {
				events.push(event);
			},
			resolveAuthHeaders(event) {
				resolverEvents.push(event);
				return { authorization: `Bearer ${canary}`, "CF-Access-Client-Secret": canary };
			},
		});
		await f.transport.request("tools/call", { name: "send", arguments: { text: "hello" } });
		expect(f.received).toHaveLength(1);
		expect(f.received[0].headers.get("Authorization")).toBe(`Bearer ${canary}`);
		expect(f.received[0].headers.get("CF-Access-Client-Secret")).toBe(canary);
		expect(f.received[0].headers.get("Content-Type")).toBe("application/json");
		expect(f.received[0].body).not.toContain(canary);
		expect(JSON.stringify(events)).not.toContain(canary);
		expect(JSON.stringify(resolverEvents)).not.toContain(canary);
		expect(Object.keys(resolverEvents[0]).sort()).toEqual(["method", "signal", "transport", "url"]);
		expect(events.map(event => event.admissionPhase)).toEqual(["prepare", "final"]);
		expect(events[0].body).toBe(events[1].body);
	});

	test("aborting a lookup stops its waiter but close waits for the underlying lookup and late credentials cannot send", async () => {
		const gate = Promise.withResolvers<Readonly<Record<string, string>>>();
		const entered = Promise.withResolvers<void>();
		const owner = new AbortController();
		const phases: string[] = [];
		const f = await httpFixture({
			signal: owner.signal,
			beforeSend(event) {
				phases.push(event.admissionPhase ?? "unphased");
			},
			resolveAuthHeaders() {
				entered.resolve();
				return gate.promise;
			},
		});
		const call = f.transport.request("tools/call", { name: "send" });
		const failed = call.catch((error: unknown) => error);
		cleanups.push(async () => gate.resolve({ Authorization: canary }));
		await entered.promise;
		owner.abort();
		expect(await failed).toMatchObject({ name: "AbortError" });
		let drained = false;
		const close = f.transport.close().then(() => {
			drained = true;
		});
		const other = await httpFixture({
			beforeSend() {},
			resolveAuthHeaders() {
				return { Authorization: "independent-owner" };
			},
		});
		await other.transport.request("ping");
		expect(other.received[0].headers.get("Authorization")).toBe("independent-owner");
		expect(drained).toBe(false);
		gate.resolve({ Authorization: canary });
		await close;
		expect(f.received).toEqual([]);
		expect(phases).toEqual(["prepare"]);
	});

	test("credential mode refuses same-origin redirects without a second lookup or fetch", async () => {
		let lookups = 0;
		const f = await httpFixture(
			{
				beforeSend() {},
				resolveAuthHeaders() {
					lookups++;
					return { Authorization: canary };
				},
			},
			true,
		);
		await expect(f.transport.request("tools/call", { name: "send" })).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(lookups).toBe(1);
		expect(f.received).toHaveLength(1);
	});

	test("a 401 cannot activate managed refresh or retransmit a credential-mode mutation", async () => {
		let resolutions = 0;
		let refreshes = 0;
		const f = await httpFixture(
			{
				beforeSend() {},
				resolveAuthHeaders() {
					resolutions++;
					return { Authorization: canary };
				},
			},
			false,
			401,
		);
		f.transport.onAuthError = async () => {
			refreshes++;
			return { Authorization: "must-not-be-used" };
		};
		await expect(
			f.transport.request("tools/call", { name: "send", arguments: { text: "one transmission" } }),
		).rejects.toThrow();
		expect(f.received).toHaveLength(1);
		expect(resolutions).toBe(1);
		expect(refreshes).toBe(0);
	});

	test("resolver exceptions are redacted and cannot become HTTP effects", async () => {
		const f = await httpFixture({
			beforeSend() {},
			resolveAuthHeaders() {
				throw new Error(canary);
			},
		});
		let failure: unknown;
		try {
			await f.transport.request("tools/call", { name: "send" });
		} catch (error) {
			failure = error;
		}
		expect(failure).toBeInstanceOf(MCPAdmissionError);
		expect(String(failure)).not.toContain(canary);
		expect(JSON.stringify(failure)).not.toContain(canary);
		expect(f.received).toEqual([]);
	});

	test("accessor, symbol and non-string resolver output refuses without evaluating private accessors", async () => {
		let accessorReads = 0;
		const outputs: Record<string, string>[] = [
			Object.defineProperty({}, "Authorization", {
				enumerable: true,
				get() {
					accessorReads++;
					return canary;
				},
			}),
			Object.defineProperty({}, Symbol("credential"), { enumerable: true, value: canary }),
			Object.defineProperty({}, "Authorization", { enumerable: true, value: 7 }),
		];
		for (const output of outputs) {
			const f = await httpFixture({
				beforeSend() {},
				resolveAuthHeaders() {
					return output;
				},
			});
			await expect(f.transport.request("tools/call", { name: "send" })).rejects.toBeInstanceOf(MCPAdmissionError);
			expect(f.received).toEqual([]);
		}
		expect(accessorReads).toBe(0);
	});

	test("late headers cannot override protocol routing or inject framing", async () => {
		const outputs: ReadonlyArray<Record<string, string>> = [
			{ Host: "wrong-host" },
			{ "Content-Length": "0" },
			{ "MCP-Protocol-Version": "wrong" },
			{ "Proxy-Authorization": canary },
			{ Authorization: "line\r\ninjected: value" },
		];
		for (const output of outputs) {
			const f = await httpFixture({
				beforeSend() {},
				resolveAuthHeaders() {
					return output;
				},
			});
			await expect(f.transport.request("tools/call", { name: "send" })).rejects.toBeInstanceOf(MCPAdmissionError);
			expect(f.received).toEqual([]);
		}
	});

	test("unsupported transports and absent admission refuse resolver mode before any lookup", () => {
		let lookups = 0;
		const options = {
			beforeSend() {},
			resolveAuthHeaders() {
				lookups++;
				return { Authorization: canary };
			},
		};
		expect(() => new StdioTransport({ command: "must-not-spawn" }, options)).toThrow(MCPAdmissionError);
		expect(() => new LegacySseTransport({ type: "sse", url: "http://127.0.0.1:1" }, options)).toThrow(
			MCPAdmissionError,
		);
		expect(
			() =>
				new HttpTransport(
					{ type: "http", url: "http://127.0.0.1:1", headerPolicy: "origin-locked" },
					{ resolveAuthHeaders: options.resolveAuthHeaders },
				),
		).toThrow(MCPAdmissionError);
		expect(lookups).toBe(0);
	});
});
