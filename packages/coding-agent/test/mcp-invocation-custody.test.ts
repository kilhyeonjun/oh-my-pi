import { afterEach, describe, expect, test } from "bun:test";
import { isRecord } from "@oh-my-pi/pi-utils";
import type { CustomToolContext } from "../src/extensibility/custom-tools/types";
import { callTool } from "../src/mcp/client";
import type { LoadMCPConfigsResult } from "../src/mcp/config";
import { MCPAdmissionError } from "../src/mcp/errors";
import { getMCPToolOrigin, permitMCPToolExecution, reserveMCPToolDispatch } from "../src/mcp/invocation";
import { MCPManager } from "../src/mcp/manager";
import { bindMCPManager, createMCPSessionBinding } from "../src/mcp/session-binding";
import { MCPTool } from "../src/mcp/tool-bridge";
import { ToolAbortError } from "../src/tools/tool-errors";
import type {
	MCPBeforeSendEvent,
	MCPHttpServerConfig,
	MCPInvocationIdentity,
	MCPToolDefinition,
	MCPTransportOptions,
} from "../src/mcp/types";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

async function fixture(
	beforeSend?: (event: MCPBeforeSendEvent) => void | Promise<void>,
	resolveAuthHeaders?: MCPTransportOptions["resolveAuthHeaders"],
	lifecycle?: { beforeAuth?: () => void | Promise<void>; failFirstCall?: boolean; changeSchemaOnReconnect?: boolean },
) {
	let initializations = 0;
	const bodies: string[] = [];
	const receivedAuth: Array<string | null> = [];
	const events: MCPBeforeSendEvent[] = [];
	const definition: MCPToolDefinition = {
		name: "send.original",
		inputSchema: {
			type: "object",
			properties: { text: { type: "string" }, optional: { type: "string" } },
			required: ["text"],
		},
	};
	const server = Bun.serve({
		port: 0,
		async fetch(request) {
			if (request.method !== "POST") return new Response(null, { status: request.method === "DELETE" ? 204 : 405 });
			const body = await request.text();
			const message: unknown = JSON.parse(body);
			if (!isRecord(message) || typeof message.method !== "string") return new Response(null, { status: 400 });
			if (message.method === "tools/call") {
				bodies.push(body);
				receivedAuth.push(request.headers.get("Authorization"));
				if (lifecycle?.failFirstCall && bodies.length === 1) return new Response(null, { status: 502 });
			}
			if (message.method === "initialize") {
				initializations++;
				if (initializations === 2 && lifecycle?.changeSchemaOnReconnect)
					definition.inputSchema = { type: "object", properties: { changed: { type: "boolean" } } };
			}
			if (message.id === undefined) return new Response(null, { status: 202 });
			const result =
				message.method === "initialize"
					? {
							protocolVersion: "2025-11-25",
							serverInfo: { name: "fixture", version: "1" },
							capabilities: { tools: {} },
						}
					: message.method === "tools/list"
						? { tools: [definition, { ...definition, name: "other" }] }
						: { content: [{ type: "text", text: "delivered" }] };
			return Response.json({ jsonrpc: "2.0", id: message.id, result });
		},
	});
	const config: MCPHttpServerConfig = {
		type: "http",
		url: `${server.url}mcp`,
		headerPolicy: "origin-locked",
		timeout: 1000,
	};
	const selected: LoadMCPConfigsResult = { configs: { "original-server": config }, sources: {}, exaApiKeys: [] };
	const binding = createMCPSessionBinding(async (): Promise<void> => {
		await manager.drainOwnedSession();
	});
	const manager = new MCPManager(
		"/",
		null,
		async () => selected,
		undefined,
		() => ({
			signal: binding.signal,
			beforeAuth() {
				return lifecycle?.beforeAuth?.();
			},
			transportOptions: {
				signal: binding.signal,
				...(resolveAuthHeaders ? { resolveAuthHeaders } : undefined),
				beforeSend: async event => {
					events.push(event);
					await beforeSend?.(event);
				},
			},
		}),
	);
	bindMCPManager(manager, binding);
	cleanups.push(async () => {
		try {
			await manager.drainOwnedSession();
		} finally {
			server.stop(true);
		}
	});
	await manager.connectServers({ "original-server": config }, {}, undefined, 0);
	const tools = manager.getTools();
	const tool = tools.find(candidate => candidate.mcpToolName === "send.original");
	const other = tools.find(candidate => candidate.mcpToolName === "other");
	if (!tool || !other) throw new Error("Native discovery did not return fixture tools");
	const execute = (id: string, params: unknown, signal?: AbortSignal) => {
		const context = {} as CustomToolContext;
		reserveMCPToolDispatch(tool, id, signal);
		permitMCPToolExecution(tool, id, context);
		return tool.execute(id, params, undefined, context, signal);
	};
	return { manager, binding, config, selected, definition, tool, other, bodies, receivedAuth, events, execute };
}

describe("native MCP invocation physical custody", () => {
	test("same-catalog read retry keeps its opaque invocation while exposed old tools remain retired", async () => {
		const f = await fixture(undefined, undefined, { failFirstCall: true });
		const result = await f.execute("read-retry", { text: "read" });
		expect(result.content).toEqual([{ type: "text", text: "delivered" }]);
		expect(f.bodies).toHaveLength(2);
		const sends = f.events.filter(event => event.association?.kind === "invocation");
		expect(sends).toHaveLength(2);
		if (sends[0].association?.kind !== "invocation" || sends[1].association?.kind !== "invocation")
			throw new Error("Missing native invocation association");
		expect(sends[0].association.invocation).toBe(sends[1].association.invocation);
		expect(() => reserveMCPToolDispatch(f.tool, "stale-after-retry")).toThrow(MCPAdmissionError);
	});

	test("changed reconnect schema refuses the active retry before a second physical callback", async () => {
		const f = await fixture(undefined, undefined, { failFirstCall: true, changeSchemaOnReconnect: true });
		await expect(f.execute("schema-retry", { text: "read" })).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(f.bodies).toHaveLength(1);
		expect(f.events.filter(event => event.association?.kind === "invocation")).toHaveLength(1);
		expect(() => reserveMCPToolDispatch(f.tool, "stale-schema")).toThrow(MCPAdmissionError);
	});

	test("host denial of mutation retransmission still reaches the same opaque invocation without another write", async () => {
		const invocations: MCPInvocationIdentity[] = [];
		const f = await fixture(
			event => {
				if (event.association?.kind !== "invocation") return;
				invocations.push(event.association.invocation);
				if (invocations.length > 1) throw new MCPAdmissionError();
			},
			undefined,
			{ failFirstCall: true },
		);
		await expect(f.execute("mutation", { text: "write once" })).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(invocations).toHaveLength(2);
		expect(invocations[0]).toBe(invocations[1]);
		expect(f.bodies).toHaveLength(1);
	});

	test("installed rediscovery retires replacement config before connected reuse or provenance changes", async () => {
		const f = await fixture();
		const connection = await f.manager.waitForConnection("original-server");
		const source = connection._source;
		f.selected.configs["original-server"] = { ...f.config, url: `${f.config.url}/replacement` };
		f.selected.sources["original-server"] = {
			provider: "replacement",
			providerName: "Replacement",
			path: "/synthetic/replacement",
			level: "project",
		};
		const result = await f.manager.discoverAndConnect({ startupTimeoutMs: 0 });
		expect(result.connectedServers).toEqual([]);
		expect(result.errors.size).toBeGreaterThan(0);
		expect(f.manager.getTools()).toEqual([]);
		expect(connection._source).toBe(source);
		expect(() => reserveMCPToolDispatch(f.tool, "replaced")).toThrow(MCPAdmissionError);
		f.selected.configs["original-server"] = f.config;
		expect((await f.manager.discoverAndConnect({ startupTimeoutMs: 0 })).connectedServers).toEqual([]);
		expect(f.bodies).toEqual([]);
	});

	test("cancelled reconnect preflight waiter cannot drain its underlying host callback", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		let held = false;
		const f = await fixture(undefined, undefined, {
			beforeAuth: () => {
				if (!held) return;
				entered.resolve();
				return release.promise;
			},
		});
		cleanups.push(async () => release.resolve());
		held = true;
		const reconnect = f.manager.reconnectServer("original-server").catch((error: unknown) => error);
		await entered.promise;
		let drained = false;
		const drain = f.manager.drainOwnedSession().then(() => {
			drained = true;
		});
		await reconnect;
		const independent = await fixture();
		await independent.execute("independent-owner", { text: "roundtrip" });
		expect(drained).toBe(false);
		release.resolve();
		await drain;
		expect(drained).toBe(true);
	});
	test("an underlying hung reconnect callback reaches native generation cleanup quarantine", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		let held = false;
		const f = await fixture(undefined, undefined, {
			beforeAuth: () => {
				if (!held) return;
				entered.resolve();
				return release.promise;
			},
		});
		cleanups.push(async () => release.resolve());
		held = true;
		const reconnect = f.manager.reconnectServer("original-server").catch((error: unknown) => error);
		await entered.promise;
		// Exercise the real native cleanup deadline, not a guessed sleep: cancellation cannot settle the callback.
		const failure = await f.binding.revoke().catch((error: unknown) => error);
		expect(failure).toMatchObject({ code: "cleanup" });
		expect(await reconnect).toBeInstanceOf(MCPAdmissionError);
		release.resolve();
		await f.manager.drainOwnedSession();
	});

	test("rediscovery observes replacement config even while a reconnect is pending", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		let held = false;
		const f = await fixture(undefined, undefined, {
			beforeAuth: () => {
				if (!held) return;
				entered.resolve();
				return release.promise;
			},
		});
		cleanups.push(async () => release.resolve());
		held = true;
		const reconnect = f.manager.reconnectServer("original-server").catch((error: unknown) => error);
		await entered.promise;
		f.selected.configs["original-server"] = { ...f.config };
		const result = await f.manager.discoverAndConnect({ startupTimeoutMs: 0 });
		expect(result.errors.size).toBe(1);
		expect(result.connectedServers).toEqual([]);
		expect(f.manager.getTools()).toEqual([]);
		expect(() => reserveMCPToolDispatch(f.tool, "pending-replacement")).toThrow(MCPAdmissionError);
		release.resolve();
		expect(await reconnect).toBeInstanceOf(MCPAdmissionError);
	});

	test("failed initialized handshake retains pending DELETE and quarantines its failure before registration", async () => {
		const deleting = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		let deletes = 0;
		const canary = "private-failed-handshake-diagnostic";
		const server = Bun.serve({
			port: 0,
			async fetch(request) {
				if (request.method === "DELETE") {
					deletes++;
					deleting.resolve();
					await release.promise;
					return new Response(canary, { status: 500 });
				}
				if (request.method !== "POST") return new Response(null, { status: 405 });
				const message: unknown = await request.json();
				if (!isRecord(message)) return new Response(null, { status: 400 });
				if (message.method === "notifications/initialized") return new Response(canary, { status: 500 });
				return Response.json(
					{
						jsonrpc: "2.0",
						id: message.id,
						result: {
							protocolVersion: "2025-11-25",
							serverInfo: { name: "failed-creation", version: "1" },
							capabilities: { tools: {} },
						},
					},
					{ headers: { "Mcp-Session-Id": "synthetic-failed-session" } },
				);
			},
		});
		const binding = createMCPSessionBinding();
		const manager = new MCPManager("/", null, undefined, undefined, () => ({
			signal: binding.signal,
			beforeAuth() {},
			transportOptions: { signal: binding.signal, beforeSend() {} },
		}));
		bindMCPManager(manager, binding);
		cleanups.push(async () => {
			release.resolve();
			try {
				await manager.drainOwnedSession();
			} catch {
				/* Expected permanent quarantine. */
			} finally {
				server.stop(true);
			}
		});
		const connecting = manager.connectServers(
			{
				failed: {
					type: "http",
					url: `${server.url}mcp`,
					headerPolicy: "origin-locked",
					timeout: 10000,
				},
			},
			{},
			undefined,
			0,
		);
		await deleting.promise;
		let drained = false;
		const drain = manager
			.drainOwnedSession()
			.then(() => {
				drained = true;
			})
			.catch((error: unknown) => error);
		const independent = await fixture();
		await independent.execute("failed-creation-independent", { text: "roundtrip" });
		expect(drained).toBe(false);
		release.resolve();
		const result = await connecting;
		const failure = await drain;
		expect(failure).toBeInstanceOf(Error);
		expect(String(failure)).toContain("drain is unconfirmed");
		expect(String(failure)).not.toContain(canary);
		expect(JSON.stringify([...result.errors])).not.toContain(canary);
		expect(drained).toBe(false);
		expect(deletes).toBe(1);
		expect(manager.getTools()).toEqual([]);
	});

	test("native manager preserves one invocation across credential prepare and final without a second write allowance", async () => {
		const phases: MCPBeforeSendEvent[] = [];
		let decisions = 0;
		let transmissions = 0;
		const canary = "native-loopback-private-header";
		const f = await fixture(
			event => {
				if (event.association?.kind !== "invocation") return;
				phases.push(event);
				if (event.admissionPhase === "prepare") decisions++;
				else if (event.admissionPhase === "final") transmissions++;
				else throw new Error("Missing credential admission phase");
			},
			() => ({ Authorization: canary }),
		);
		await f.execute("phased", { text: "prepared", optional: "", i: "intent" });
		expect(decisions).toBe(1);
		expect(transmissions).toBe(1);
		expect(phases).toHaveLength(2);
		expect(phases[0].association).toBe(phases[1].association);
		expect(phases[0].wireId).toBe(phases[1].wireId);
		expect(phases[0].body).toBe(f.bodies[0]);
		expect(f.receivedAuth).toEqual([canary]);
		expect(JSON.stringify(phases)).not.toContain(canary);
	});

	test("only native dispatch sends prepared arguments; handles never appear on the HTTP wire", async () => {
		const f = await fixture();
		await expect(f.tool.execute("raw", { text: "hello" }, undefined, {} as CustomToolContext)).rejects.toBeInstanceOf(
			MCPAdmissionError,
		);
		await f.execute("logical-call", { text: "hello", optional: "", i: "private harness intent" });
		expect(f.bodies).toHaveLength(1);
		const body: unknown = JSON.parse(f.bodies[0]);
		expect(body).toEqual({
			jsonrpc: "2.0",
			id: expect.any(Number),
			method: "tools/call",
			params: { name: "send.original", arguments: { text: "hello" } },
		});
		const event = f.events.find(candidate => candidate.association?.kind === "invocation");
		expect(event?.body).toBe(f.bodies[0]);
		expect(isRecord(body) ? body.id : undefined).toBe(event?.wireId);
		const origin = getMCPToolOrigin(f.tool);
		if (!origin) throw new Error("Native fixture tool lost its original identity");
		expect(event?.association?.kind === "invocation" && event.association.invocation.origin).toBe(origin);
		const connection = await f.manager.waitForConnection("original-server");
		const invocation = event?.association?.kind === "invocation" ? event.association.invocation : undefined;
		await expect(callTool(connection, "send.original", { text: "stolen" }, { invocation })).rejects.toBeInstanceOf(
			MCPAdmissionError,
		);
		expect(f.bodies).toHaveLength(1);
	});

	test("copied contexts, another native tool, replayed ids and retired catalog objects cannot borrow execution", async () => {
		const f = await fixture();
		const context = {} as CustomToolContext;
		reserveMCPToolDispatch(f.tool, "reserved");
		permitMCPToolExecution(f.tool, "reserved", context);
		await expect(f.tool.execute("reserved", { text: "copy" }, undefined, { ...context })).rejects.toBeInstanceOf(
			MCPAdmissionError,
		);
		await expect(f.other.execute("reserved", { text: "wrong tool" }, undefined, context)).rejects.toBeInstanceOf(
			MCPAdmissionError,
		);
		expect(() => reserveMCPToolDispatch(f.tool, "reserved")).toThrow(MCPAdmissionError);
		const connection = await f.manager.waitForConnection("original-server");
		const forgedTool = new MCPTool(connection, f.definition);
		await expect(
			forgedTool.execute("forged", { text: "not in catalog" }, undefined, {} as CustomToolContext),
		).rejects.toBeInstanceOf(MCPAdmissionError);
		await f.manager.refreshServerTools("original-server");
		expect(() => reserveMCPToolDispatch(f.tool, "stale")).toThrow(MCPAdmissionError);
		expect(f.bodies).toEqual([]);
	});

	test("concurrent identical calls have independent opaque identities and per-call cancellation", async () => {
		const pending = Promise.withResolvers<void>();
		const firstEntered = Promise.withResolvers<void>();
		const seen: MCPInvocationIdentity[] = [];
		const f = await fixture(async event => {
			if (event.association?.kind !== "invocation") return;
			seen.push(event.association.invocation);
			if (seen.length === 1) {
				firstEntered.resolve();
				await pending.promise;
			}
		});
		const cancel = new AbortController();
		const first = f.execute("first", { text: "same" }, cancel.signal);
		const firstFailure = first.catch((error: unknown) => error);
		cleanups.push(async () => pending.resolve());
		await firstEntered.promise;
		await f.execute("second", { text: "same" });
		cancel.abort();
		pending.resolve();
		expect(await firstFailure).toBeInstanceOf(ToolAbortError);
		expect(seen).toHaveLength(2);
		expect(seen[0]).not.toBe(seen[1]);
		expect(f.bodies).toHaveLength(1);
		expect(seen[0].signal.aborted).toBe(true);
	});

	test("a duplicate live call id revokes the first pending send rather than lending its approval", async () => {
		const pending = Promise.withResolvers<void>();
		const entered = Promise.withResolvers<void>();
		const f = await fixture(async event => {
			if (event.association?.kind === "invocation") {
				entered.resolve();
				await pending.promise;
			}
		});
		const first = f.execute("duplicate", { text: "held" });
		const failure = first.catch((error: unknown) => error);
		cleanups.push(async () => pending.resolve());
		await entered.promise;
		expect(() => reserveMCPToolDispatch(f.tool, "duplicate")).toThrow(MCPAdmissionError);
		pending.resolve();
		expect(await failure).toBeInstanceOf(MCPAdmissionError);
		expect(f.bodies).toEqual([]);
	});

	test("execution permits cannot cross native profile configurations", async () => {
		const first = await fixture();
		const second = await fixture();
		const context = {} as CustomToolContext;
		reserveMCPToolDispatch(first.tool, "profile-bound");
		permitMCPToolExecution(first.tool, "profile-bound", context);
		await expect(
			second.tool.execute("profile-bound", { text: "wrong profile" }, undefined, context),
		).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(first.bodies).toEqual([]);
		expect(second.bodies).toEqual([]);
	});

	test("owner revocation during physical approval blocks a late allow and all old origins", async () => {
		const pending = Promise.withResolvers<void>();
		const entered = Promise.withResolvers<void>();
		const f = await fixture(async event => {
			if (event.association?.kind === "invocation") {
				entered.resolve();
				await pending.promise;
			}
		});
		const call = f.execute("held", { text: "never delivered" });
		const rejected = call.catch((error: unknown) => error);
		cleanups.push(async () => pending.resolve());
		await entered.promise;
		const revoked = f.binding.revoke();
		pending.resolve();
		expect(await rejected).toBeInstanceOf(ToolAbortError);
		await revoked;
		expect(f.bodies).toEqual([]);
		expect(() => reserveMCPToolDispatch(f.tool, "after-revoke")).toThrow(MCPAdmissionError);
	});
});
