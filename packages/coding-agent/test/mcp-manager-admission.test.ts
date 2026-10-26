import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test, vi } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { AuthStorage, SqliteAuthCredentialStore } from "@oh-my-pi/pi-ai";
import { invalidateCommandConfig } from "@oh-my-pi/pi-coding-agent/config/resolve-config-value";
import { callTool } from "@oh-my-pi/pi-coding-agent/mcp/client";
import { MCPAdmissionError } from "@oh-my-pi/pi-coding-agent/mcp/errors";
import { MCPManager } from "@oh-my-pi/pi-coding-agent/mcp/manager";
import { DeferredMCPTool, MCPTool } from "@oh-my-pi/pi-coding-agent/mcp/tool-bridge";
import type { MCPBeforeAuthEvent, MCPServerConfig, MCPToolDefinition } from "@oh-my-pi/pi-coding-agent/mcp/types";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	vi.useRealTimers();
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

const definition: MCPToolDefinition = {
	name: "read_counter",
	description: "Read fixture counter",
	inputSchema: { type: "object", properties: {} },
};

async function fixture() {
	const root = await fs.mkdtemp(path.join(os.tmpdir(), "mcp-manager-admission-"));
	const marker = path.join(root, "header-command");
	const command = `!printf prepared > '${marker}'; printf fixture-header`;
	const auth = new AuthStorage(new SqliteAuthCredentialStore(new Database(":memory:")));
	await auth.credentials.reload();
	const rotated = Promise.withResolvers<void>();
	const stopObservingStorage = auth.credentials.onGeneration(() => {
		const credential = auth.credentials.get("fixture-admission");
		if (credential?.type === "oauth" && credential.access === "fixture-fresh") rotated.resolve();
	});
	const owner = new AbortController();
	const phases: string[] = [];
	let denied: MCPBeforeAuthEvent["phase"] | undefined;
	let preflightGate: Promise<void> | undefined;
	const preflightStarted = Promise.withResolvers<void>();
	let tokenRequests = 0;
	let mcpRequests = 0;
	let unauthorized = false;
	let denyPhysicalCall = false;
	let receivedHeader: string | null = null;
	let holdToken: Promise<void> | undefined;
	const tokenStarted = Promise.withResolvers<void>();
	const server = Bun.serve({
		port: 0,
		async fetch(request) {
			if (new URL(request.url).pathname === "/token") {
				tokenRequests++;
				tokenStarted.resolve();
				if (holdToken) await holdToken;
				return Response.json({
					access_token: "fixture-fresh",
					refresh_token: "fixture-rotated",
					token_type: "Bearer",
					expires_in: 3600,
				});
			}
			mcpRequests++;
			receivedHeader = request.headers.get("X-Fixture");
			if (request.method === "DELETE") return new Response(null, { status: 204 });
			if (request.method !== "POST") return new Response(null, { status: 405 });
			const message = (await request.json()) as { id?: number; method: string };
			if (unauthorized && message.method === "tools/call") return new Response(null, { status: 401 });
			if (message.id === undefined) return new Response(null, { status: 202 });
			const result =
				message.method === "initialize"
					? {
							protocolVersion: "2025-03-26",
							serverInfo: { name: "admission-fixture", version: "1" },
							capabilities: { tools: {} },
						}
					: message.method === "tools/list"
						? { tools: [definition] }
						: { content: [{ type: "text", text: "fixture read completed" }] };
			return Response.json({ jsonrpc: "2.0", id: message.id, result });
		},
	});
	const config: MCPServerConfig = {
		type: "http",
		url: `${server.url}mcp`,
		headers: { "X-Fixture": command },
		timeout: 1000,
		auth: { type: "oauth", credentialId: "fixture-admission", tokenUrl: `${server.url}token` },
	};
	const seed = () =>
		auth.credentials.set("fixture-admission", {
			type: "oauth",
			access: "fixture-stale",
			refresh: "fixture-refresh",
			expires: Date.now() - 60_000,
		});
	await seed();
	let bindings = 0;
	const manager = new MCPManager(root, null, undefined, { ladderMs: [1, 1], retryBaseMs: 1, retryMaxMs: 2 }, () => {
		bindings++;
		return {
			signal: owner.signal,
			transportOptions: {
				beforeSend(event) {
					if (denyPhysicalCall && event.body && JSON.parse(event.body).method === "tools/call")
						throw new Error("private physical denial");
				},
			},
			beforeAuth: event => {
				phases.push(event.phase);
				preflightStarted.resolve();
				if (event.phase === denied) throw new Error("private fixture denial");
				return preflightGate;
			},
		};
	});
	manager.setAuthStorage(auth);
	cleanups.push(async () => {
		await manager.disconnectAll();
		server.stop(true);
		auth.close();
		stopObservingStorage();
		invalidateCommandConfig(command);
		await fs.rm(root, { recursive: true, force: true });
	});
	return {
		manager,
		owner,
		config,
		phases,
		auth,
		marker,
		command,
		seed,
		tokenStarted,
		rotated,
		preflightStarted,
		holdPreflight: (gate: Promise<void>) => {
			preflightGate = gate;
		},
		deny: (phase: MCPBeforeAuthEvent["phase"]) => {
			denied = phase;
		},
		setUnauthorized: () => {
			unauthorized = true;
		},
		denyPhysicalCall: () => {
			denyPhysicalCall = true;
		},
		holdToken: (gate: Promise<void>) => {
			holdToken = gate;
		},
		counts: () => ({ tokenRequests, mcpRequests, bindings }),
		header: () => receivedHeader,
	};
}

describe("MCPManager source-bound admission effect boundaries", () => {
	test("physical call denial keeps native identity during retirement and cannot replay or refresh", async () => {
		const f = await fixture();
		await f.manager.connectServers({ fixture: f.config }, {}, undefined, 0);
		const connection = await f.manager.waitForConnection("fixture");
		const before = f.counts();
		f.denyPhysicalCall();
		await expect(callTool(connection, "read_counter", {})).rejects.toBeInstanceOf(MCPAdmissionError);
		await expect(f.manager.reconnectServer("fixture")).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(f.counts()).toEqual(before);
	});

	test("initial denial runs no native OAuth, command, or connection effects and cannot renew", async () => {
		const f = await fixture();
		f.deny("initial");
		const result = await f.manager.connectServers({ fixture: f.config }, {}, undefined, 0);
		expect(result.errors.has("fixture")).toBe(true);
		expect(f.counts()).toEqual({ tokenRequests: 0, mcpRequests: 0, bindings: 1 });
		expect(await Bun.file(f.marker).exists()).toBe(false);
		await expect(f.manager.reconnectServer("fixture", { manual: true })).rejects.toBeInstanceOf(MCPAdmissionError);
		await expect(f.manager.prepareConfig(f.config, { serverName: "fixture" })).rejects.toBeInstanceOf(
			MCPAdmissionError,
		);
		expect(f.phases).toEqual(["initial"]);
	});

	test("stdio preflight denial executes neither command-backed env nor the child", async () => {
		const f = await fixture();
		const childMarker = `${f.marker}-child`;
		const config: MCPServerConfig = {
			type: "stdio",
			command: process.execPath,
			args: ["-e", `require('node:fs').writeFileSync(${JSON.stringify(childMarker)}, 'spawned')`],
			env: { FIXTURE_AUTH: f.command },
			timeout: 1000,
		};
		f.deny("initial");
		const result = await f.manager.connectServers({ fixture: config }, {}, undefined, 0);
		expect(result.errors.has("fixture")).toBe(true);
		expect(await Bun.file(f.marker).exists()).toBe(false);
		expect(await Bun.file(childMarker).exists()).toBe(false);
		expect(f.counts().mcpRequests).toBe(0);
	});

	for (const phase of ["prepare", "reconnect", "auth-refresh"] as const) {
		test(`${phase} denial stops native effects and every subsequent retry`, async () => {
			const f = await fixture();
			await f.manager.connectServers({ fixture: f.config }, {}, undefined, 0);
			expect(f.header()).toBe("fixture-header");
			const permitted = await f.manager.getTools()[0]!.execute("permitted", {}, undefined, {} as never);
			expect(permitted.content).toEqual([{ type: "text", text: "fixture read completed" }]);
			expect(f.auth.credentials.get("fixture-admission")).toMatchObject({
				access: "fixture-fresh",
				refresh: "fixture-rotated",
			});
			await fs.rm(f.marker);
			invalidateCommandConfig(f.command);
			await f.seed();
			f.deny(phase);
			const before = f.counts();
			vi.useFakeTimers();
			if (phase === "prepare") {
				await expect(f.manager.prepareConfig(f.config, { serverName: "fixture" })).rejects.toBeInstanceOf(
					MCPAdmissionError,
				);
			} else if (phase === "reconnect") {
				await expect(f.manager.reconnectServer("fixture")).rejects.toBeInstanceOf(MCPAdmissionError);
			} else {
				f.setUnauthorized();
				await expect(
					f.manager.getTools()[0]!.execute("fixture-call", {}, undefined, {} as never),
				).rejects.toBeInstanceOf(MCPAdmissionError);
			}
			await expect(f.manager.reconnectServer("fixture")).rejects.toBeInstanceOf(MCPAdmissionError);
			try {
				vi.advanceTimersByTime(15);
				await Promise.resolve();
			} finally {
				vi.useRealTimers();
			}
			expect(f.counts().tokenRequests).toBe(before.tokenRequests);
			expect(f.counts().bindings).toBe(1);
			expect(await Bun.file(f.marker).exists()).toBe(false);
			expect(f.phases.filter(value => value === phase)).toHaveLength(1);
			// The auth-error case has exactly one admitted request which received 401, not a replay.
			expect(f.counts().mcpRequests).toBe(before.mcpRequests + (phase === "auth-refresh" ? 1 : 0));
		});
	}

	test("revocation during native refresh settles consumers, stores rotation, and never connects late", async () => {
		const f = await fixture();
		const gate = Promise.withResolvers<void>();
		f.holdToken(gate.promise);
		const loading = f.manager.connectServers({ fixture: f.config }, {}, undefined, 0);
		await f.tokenStarted.promise;
		f.owner.abort();
		await loading;
		await expect(f.manager.waitForConnection("fixture")).rejects.toBeInstanceOf(MCPAdmissionError);
		gate.resolve();
		// Observe completion of the real native storage write, not a simulated refresh result.
		await f.rotated.promise;
		expect(f.auth.credentials.get("fixture-admission")).toMatchObject({
			access: "fixture-fresh",
			refresh: "fixture-rotated",
		});
		expect(f.counts().mcpRequests).toBe(0);
		expect(await Bun.file(f.marker).exists()).toBe(false);
	});

	test("revoked live ownership forbids fresh preparation, reconnect, and auth replay", async () => {
		const f = await fixture();
		await f.manager.connectServers({ fixture: f.config }, {}, undefined, 0);
		const cached = f.manager.getTools()[0]!;
		const before = f.counts();
		await fs.rm(f.marker);
		invalidateCommandConfig(f.command);
		f.owner.abort();
		await expect(f.manager.prepareConfig(f.config, { serverName: "fixture" })).rejects.toBeInstanceOf(
			MCPAdmissionError,
		);
		await expect(f.manager.reconnectServer("fixture")).rejects.toBeInstanceOf(MCPAdmissionError);
		await expect(cached.execute("revoked", {}, undefined, {} as never)).rejects.toThrow();
		expect(f.phases).toEqual(["initial"]);
		expect(f.counts()).toEqual(before);
		expect(await Bun.file(f.marker).exists()).toBe(false);
	});

	test("revocation settles a nonsettling preflight and late allow remains inert", async () => {
		const f = await fixture();
		const gate = Promise.withResolvers<void>();
		f.holdPreflight(gate.promise);
		const loading = f.manager.connectServers({ fixture: f.config }, {}, undefined, 0);
		await f.preflightStarted.promise;
		f.owner.abort();
		await loading;
		gate.resolve();
		await gate.promise;
		await f.manager.waitForPendingConnections();
		await expect(f.manager.reconnectServer("fixture")).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(f.counts()).toEqual({ tokenRequests: 0, mcpRequests: 0, bindings: 1 });
		expect(await Bun.file(f.marker).exists()).toBe(false);
	});

	test("missing public preparation ownership fails closed before native work", async () => {
		const f = await fixture();
		await expect(f.manager.prepareConfig(f.config)).rejects.toBeInstanceOf(MCPAdmissionError);
		await expect(f.manager.prepareConfig(f.config, { serverName: "unknown" })).rejects.toBeInstanceOf(
			MCPAdmissionError,
		);
		expect(f.counts()).toEqual({ tokenRequests: 0, mcpRequests: 0, bindings: 0 });
		expect(await Bun.file(f.marker).exists()).toBe(false);
	});

	test("cached/deferred native tools do not hide reconnect denial or retry it twice", async () => {
		const f = await fixture();
		await f.manager.connectServers({ fixture: f.config }, {}, undefined, 0);
		const connection = await f.manager.waitForConnection("fixture");
		f.deny("reconnect");
		await expect(f.manager.reconnectServer("fixture")).rejects.toBeInstanceOf(MCPAdmissionError);
		let retries = 0;
		const reconnect = () => {
			retries++;
			return f.manager.reconnectServer("fixture");
		};
		const deferred = new DeferredMCPTool(
			"fixture",
			definition,
			() => f.manager.waitForConnection("fixture"),
			undefined,
			reconnect,
		);
		await expect(deferred.execute("deferred", {}, undefined, {} as never)).rejects.toBeInstanceOf(MCPAdmissionError);
		expect(retries).toBe(0);
		const active = new MCPTool(connection, definition, reconnect);
		// The stale connection's transport is closed. Its one recovery attempt meets the same terminal owner.
		await expect(active.execute("cached", {}, undefined, {} as never)).rejects.toThrow();
		expect(retries).toBeLessThanOrEqual(1);
		expect(f.phases).toEqual(["initial", "reconnect"]);
	});
});
