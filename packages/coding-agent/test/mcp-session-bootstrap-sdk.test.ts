import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { AuthStorage } from "@oh-my-pi/pi-ai";
import { Type } from "@oh-my-pi/omptype/typebox";
import { getBundledModel } from "@oh-my-pi/pi-catalog/models";
import { getAgentDir, setAgentDir } from "@oh-my-pi/pi-utils";
import { ModelRegistry } from "../src/config/model-registry";
import { Settings } from "../src/config/settings";
import type { ExtensionFactory } from "../src/extensibility/extensions/types";
import type { MCPSessionBinding, MCPSessionBootstrapInstallation } from "../src/mcp/bootstrap";
import { MCPBootstrapError } from "../src/mcp/bootstrap";
import { getMCPManagerBinding } from "../src/mcp/session-binding";
import { getMCPToolOrigin } from "../src/mcp/invocation";
import { createMCPToolName } from "../src/mcp/tool-bridge";
import { createExtensionDashboardRuntime } from "../src/modes/components/extensions/dashboard-runtime";
import { applyMcpToggleRuntime } from "../src/modes/components/extensions/mcp-runtime";
import { createAgentSession, type CreateAgentSessionOptions } from "../src/sdk";
import type { AgentSession } from "../src/session/agent-session";
import { SessionManager } from "../src/session/session-manager";
import { assistantMsg } from "./utilities";

const sessions: AgentSession[] = [];
const authStores: AuthStorage[] = [];
const dirs: string[] = [];
const servers: Bun.Server<undefined>[] = [];
const releaseGates: Array<() => void> = [];
afterEach(async () => {
	for (const release of releaseGates.splice(0)) release();
	for (const session of sessions.splice(0)) await session.dispose();
	for (const server of servers.splice(0)) server.stop(true);
	for (const auth of authStores.splice(0)) auth.close();
	for (const dir of dirs.splice(0)) await fs.rm(dir, { recursive: true, force: true });
});

async function options(factory: ExtensionFactory): Promise<CreateAgentSessionOptions> {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "omp-native-mcp-sdk-"));
	dirs.push(dir);
	const authStorage = await AuthStorage.create(path.join(dir, "auth.db"));
	authStores.push(authStorage);
	authStorage.keys.setRuntime("openai", "test-key");
	return {
		cwd: dir,
		agentDir: dir,
		authStorage,
		modelRegistry: new ModelRegistry(authStorage),
		model: getBundledModel("openai", "gpt-4o-mini"),
		sessionManager: SessionManager.inMemory(dir),
		settings: Settings.isolated({ "autolearn.enabled": false }),
		toolNames: [],
		skills: [],
		contextFiles: [],
		promptTemplates: [],
		slashCommands: [],
		disableExtensionDiscovery: true,
		enableLsp: false,
		skipPythonPreflight: true,
		preloadedPreparedExtensions: [{ path: import.meta.path, resolvedPath: import.meta.path, factory, error: null }],
		mcpBootstrap: { extensionPath: import.meta.path },
	};
}

const nativeToolName = createMCPToolName("selected", "read_counter");
const notificationMethod = "notifications/custom/native-generation";

function nativeCatalog() {
	let generations = 0;
	let calls = 0;
	const server = Bun.serve({
		port: 0,
		async fetch(request) {
			if (request.method === "DELETE") return new Response(null, { status: 204 });
			if (request.method !== "POST") return new Response(null, { status: 405 });
			const message = (await request.json()) as { id?: number; method: string };
			if (message.id === undefined) return new Response(null, { status: 202 });
			if (message.method === "initialize") {
				generations++;
				return Response.json({
					jsonrpc: "2.0",
					id: message.id,
					result: {
						protocolVersion: "2025-03-26",
						serverInfo: { name: "native-sdk-fixture", version: "1" },
						capabilities: { tools: {} },
					},
				});
			}
			if (message.method === "tools/list") {
				const notification = { jsonrpc: "2.0", method: notificationMethod, params: { generation: generations } };
				const response = {
					jsonrpc: "2.0",
					id: message.id,
					result: {
						tools: [
							{
								name: "read_counter",
								description: "Read the selected native counter",
								inputSchema: { type: "object", properties: {} },
								annotations: { readOnlyHint: true },
							},
						],
					},
				};
				return new Response(`data: ${JSON.stringify(notification)}\n\ndata: ${JSON.stringify(response)}\n\n`, {
					headers: { "Content-Type": "text/event-stream" },
				});
			}
			if (message.method === "tools/call") calls++;
			return Response.json({
				jsonrpc: "2.0",
				id: message.id,
				result: {
					content: [{ type: "text", text: String(calls) }],
				},
			});
		},
	});
	servers.push(server);
	return {
		config: { type: "http" as const, url: `${server.url}mcp`, headerPolicy: "origin-locked" as const, timeout: 1000 },
		get calls() {
			return calls;
		},
		get generations() {
			return generations;
		},
	};
}

function initializeNotifications(session: AgentSession): void {
	const runner = session.extensionRunner;
	if (!runner) throw new Error("Missing native extension runner");
	runner.initialize(
		{
			sendMessage: () => {},
			sendUserMessage: () => {},
			appendEntry: () => {},
			setLabel: () => {},
			getActiveTools: () => session.getActiveToolNames(),
			getAllTools: () => session.getAllToolInfos(),
			setActiveTools: names => session.setActiveToolsByName(names),
			getCommands: () => [],
			setModel: async () => false,
			getThinkingLevel: () => undefined,
			setThinkingLevel: () => {},
			getSessionName: () => undefined,
			setSessionName: async () => {},
		},
		{
			getModel: () => session.model,
			isIdle: () => true,
			abort: () => {},
			hasPendingMessages: () => false,
			shutdown: () => {},
			getContextUsage: () => undefined,
			compact: async () => {},
			getSystemPrompt: () => [],
		},
	);
}

describe("native SDK selected MCP bootstrap", () => {
	it("replaces the real UI pending entry with the admitted selected catalog", async () => {
		const remote = nativeCatalog();
		const release = Promise.withResolvers<void>();
		releaseGates.push(release.resolve);
		const opts = await options(api =>
			api.registerMCPSessionBootstrap(context => ({
				loadConfigs: async () => {
					await release.promise;
					return { configs: { selected: remote.config }, sources: {}, exaApiKeys: [] };
				},
				admission: () => ({
					signal: context.binding.signal,
					beforeAuth: () => {},
					transportOptions: { beforeSend: event => event.signal.throwIfAborted() },
				}),
				dispose: () => {},
			})),
		);
		const { session } = await createAgentSession({
			...opts,
			hasUI: true,
			autoApprove: true,
			toolNames: [nativeToolName],
		});
		sessions.push(session);
		const provisional = session.agent.state.tools.find(tool => tool.name === nativeToolName);
		if (!provisional) throw new Error("Explicit native tool has no provisional entry");
		expect(getMCPToolOrigin(provisional)).toBeUndefined();
		const published = Promise.withResolvers<void>();
		const unsubscribe = session.getNativeMCPManager()?.addConnectionStatusListener(event => {
			if (event.type === "connected" && event.serverName === "selected") published.resolve();
		});
		release.resolve();
		await published.promise;
		unsubscribe?.();
		const admitted = session.agent.state.tools.find(tool => tool.name === nativeToolName);
		expect(admitted).not.toBe(provisional);
		if (!admitted) throw new Error("Admitted native tool was not published");
		const result = await admitted.execute("selected-published-read", {});
		expect(result.content).toContainEqual({ type: "text", text: "1" });
		expect(session.getNativeMCPManager()?.getConnectionStatus("selected")).toBe("connected");
		expect(getMCPManagerBinding(session.getNativeMCPManager())?.signal.aborted).toBe(false);
		expect(remote.generations).toBe(1);
		expect(remote.calls).toBe(1);
	});

	for (const source of ["extension", "user"] as const) {
		it(`does not replace a ${source} tool masquerading as a native pending entry`, async () => {
			const remote = nativeCatalog();
			const release = Promise.withResolvers<void>();
			releaseGates.push(release.resolve);
			const spoof = {
				name: nativeToolName,
				label: "selected/read_counter",
				description: "Pending MCP tool. Not owned by native discovery",
				parameters: Type.Object({}),
				mcpServerName: "selected",
				mcpToolName: "read_counter",
				async execute() {
					throw new Error("Spoof must not execute");
				},
			};
			const opts = await options(api => {
				if (source === "extension") api.registerTool(spoof);
				api.registerMCPSessionBootstrap(context => ({
					loadConfigs: async () => {
						await release.promise;
						return { configs: { selected: remote.config }, sources: {}, exaApiKeys: [] };
					},
					admission: () => ({
						signal: context.binding.signal,
						beforeAuth: () => {},
						transportOptions: { beforeSend: event => event.signal.throwIfAborted() },
					}),
					dispose: () => {},
				}));
			});
			if (source === "user") opts.customTools = [spoof];
			const { session } = await createAgentSession({ ...opts, hasUI: true, toolNames: [nativeToolName] });
			sessions.push(session);
			const binding = getMCPManagerBinding(session.getNativeMCPManager());
			if (!binding) throw new Error("Missing selected native binding");
			const revoked = Promise.withResolvers<void>();
			binding.signal.addEventListener("abort", () => revoked.resolve(), { once: true });
			release.resolve();
			await revoked.promise;
			await session.setActiveToolsByName([nativeToolName]);
			expect(binding.signal.aborted).toBe(true);
			expect(getMCPToolOrigin(session.agent.state.tools.find(tool => tool.name === nativeToolName))).toBeUndefined();
			expect(remote.calls).toBe(0);
		});
	}

	it("does not publish a native manager before factory installation and uses fresh authority after cancelled new-session hooks", async () => {
		const install = Promise.withResolvers<MCPSessionBootstrapInstallation>();
		const entered = Promise.withResolvers<void>();
		const bindings: MCPSessionBinding[] = [];
		let loads = 0;
		let disposals = 0;
		let hookSawRevoked = false;
		const installation: MCPSessionBootstrapInstallation = {
			loadConfigs: async () => {
				loads++;
				return { configs: {}, sources: {}, exaApiKeys: [] };
			},
			admission: () => {
				throw new Error("No server may authenticate in an empty catalog");
			},
			dispose: () => {
				disposals++;
			},
		};
		const opts = await options(api => {
			api.registerMCPSessionBootstrap(context => {
				bindings.push(context.binding);
				entered.resolve();
				return bindings.length === 1 ? install.promise : installation;
			});
			api.on("session_before_switch", () => {
				hookSawRevoked = bindings[0].signal.aborted;
				return { cancel: true };
			});
		});
		const creating = createAgentSession(opts);
		await entered.promise;
		expect(loads).toBe(0);
		install.resolve(installation);
		const result = await creating;
		sessions.push(result.session);
		expect(getMCPManagerBinding(result.mcpManager)).toBe(bindings[0]);
		expect(loads).toBe(1);
		const oldSessionId = result.session.sessionManager.getSessionId();
		expect(await result.session.newSession()).toBe(false);
		expect(hookSawRevoked).toBe(true);
		expect(result.session.sessionManager.getSessionId()).toBe(oldSessionId);
		expect(bindings).toHaveLength(2);
		expect(bindings[1]).not.toBe(bindings[0]);
		expect(bindings[1].signal.aborted).toBe(false);
		expect(loads).toBe(2);
		expect(disposals).toBe(1);
	});

	it("forwards each fresh generation once across fork, entry-fork, branch, BTW, switch and reload cancellation", async () => {
		const remote = nativeCatalog();
		const notifications: unknown[] = [];
		let received = Promise.withResolvers<void>();
		const bindings: MCPSessionBinding[] = [];
		let hookChecks = 0;
		let cancelTransitions = true;
		const opts = await options(api => {
			api.registerMCPSessionBootstrap(context => {
				bindings.push(context.binding);
				return {
					loadConfigs: async () => ({ configs: { selected: remote.config }, sources: {}, exaApiKeys: [] }),
					admission: () => ({
						signal: context.binding.signal,
						beforeAuth: () => {},
						transportOptions: { beforeSend: event => event.signal.throwIfAborted() },
					}),
					dispose: () => {},
				};
			});
			const cancel = () => {
				expect(bindings[bindings.length - 1].signal.aborted).toBe(true);
				hookChecks++;
				return cancelTransitions ? { cancel: true } : undefined;
			};
			api.on("session_before_switch", cancel);
			api.on("session_before_branch", cancel);
			api.on("mcp_notification", event => {
				if (event.method !== notificationMethod) return;
				notifications.push(event.params);
				received.resolve();
			});
		});
		if (!opts.cwd) throw new Error("Missing test workspace");
		opts.sessionManager = SessionManager.create(opts.cwd, path.join(opts.cwd, "sessions"));
		const { session } = await createAgentSession(opts);
		sessions.push(session);
		initializeNotifications(session);
		await received.promise;
		expect(notifications).toEqual([{ generation: 1 }]);
		const userId = session.sessionManager.appendMessage({ role: "user", content: "seed", timestamp: Date.now() });
		const leafId = session.sessionManager.appendMessage(assistantMsg("answer"));
		session.agent.replaceMessages(session.sessionManager.buildSessionContext().messages);
		await session.sessionManager.flush();
		const sessionFile = session.sessionFile;
		if (!sessionFile) throw new Error("Missing persisted test session");
		const sessionId = session.sessionManager.getSessionId();
		const transitions: Array<() => Promise<void>> = [
			async () => {
				expect(await session.newSession()).toBe(false);
			},
			async () => {
				expect(await session.fork()).toBe(false);
			},
			async () => {
				expect(await session.fork(leafId)).toBe(false);
			},
			async () => {
				expect((await session.branch(userId)).cancelled).toBe(true);
			},
			async () => {
				expect(
					(await session.branchFromBtw("side", assistantMsg("side answer"), leafId, sessionId)).cancelled,
				).toBe(true);
			},
			async () => {
				expect(await session.switchSession(sessionFile)).toBe(false);
			},
			async () => {
				await expect(session.reload()).rejects.toThrow();
			},
		];
		for (const transition of transitions) {
			const outgoing = bindings[bindings.length - 1];
			const before = bindings.length;
			received = Promise.withResolvers<void>();
			await transition();
			await received.promise;
			expect(notifications).toHaveLength(before + 1);
			expect(notifications[before]).toEqual({ generation: before + 1 });
			expect(outgoing.signal.aborted).toBe(true);
			expect(bindings).toHaveLength(before + 1);
			expect(bindings[before].signal.aborted).toBe(false);
			expect(session.sessionManager.getSessionId()).toBe(sessionId);
		}
		expect(hookChecks).toBe(7);
		received = Promise.withResolvers<void>();
		await session.refreshNativeMCP();
		await received.promise;
		expect(notifications).toHaveLength(9);
		expect(notifications[8]).toEqual({ generation: 9 });
		cancelTransitions = false;
		received = Promise.withResolvers<void>();
		expect(await session.newSession()).toBe(true);
		await received.promise;
		expect(notifications).toHaveLength(10);
		expect(notifications[9]).toEqual({ generation: 10 });
	}, 30_000);

	it("dashboard resolves the rotated owner and refuses persistence, ambient discovery and disconnect", async () => {
		const remote = nativeCatalog();
		const opts = await options(api =>
			api.registerMCPSessionBootstrap(context => ({
				loadConfigs: async () => ({ configs: { selected: remote.config }, sources: {}, exaApiKeys: [] }),
				admission: () => ({
					signal: context.binding.signal,
					beforeAuth: () => {},
					transportOptions: { beforeSend: event => event.signal.throwIfAborted() },
				}),
				dispose: () => {},
			})),
		);
		if (!opts.cwd || !opts.agentDir || !opts.settings) throw new Error("Missing isolated dashboard workspace");
		const originalAgentDir = getAgentDir();
		setAgentDir(opts.agentDir);
		releaseGates.push(() => setAgentDir(originalAgentDir));
		const sourcePath = path.join(opts.cwd, "mcp.json");
		const originalConfig = JSON.stringify({ mcpServers: { selected: remote.config } });
		await fs.writeFile(sourcePath, originalConfig);
		const { session } = await createAgentSession(opts);
		sessions.push(session);
		const runtime = createExtensionDashboardRuntime({
			cwd: opts.cwd,
			settings: opts.settings,
			getMcpManager: () => session.getNativeMCPManager(),
			onMcpToolsChanged: tools => session.refreshMCPTools(tools),
		});
		const outgoing = runtime.mcpSource?.getConnection("selected");
		await session.refreshNativeMCP();
		const manager = session.getNativeMCPManager();
		if (!manager) throw new Error("Missing replacement manager");
		expect(runtime.mcpSource?.getConnection("selected")).not.toBe(outgoing);
		expect(runtime.mcpSource?.getConnectionStatus("selected")).toBe("connected");
		let ambientLoads = 0;
		for (const enabled of [false, true]) {
			await expect(runtime.persistMcpToggle("selected", enabled, sourcePath)).rejects.toBeInstanceOf(
				MCPBootstrapError,
			);
			await expect(runtime.applyMcpToggle("selected", enabled)).rejects.toBeInstanceOf(MCPBootstrapError);
			await expect(
				applyMcpToggleRuntime({
					cwd: opts.cwd,
					name: "selected",
					enabled,
					manager,
					loadConfigs: async () => {
						ambientLoads++;
						return { configs: {}, sources: {}, exaApiKeys: [] };
					},
				}),
			).rejects.toBeInstanceOf(MCPBootstrapError);
		}
		expect(ambientLoads).toBe(0);
		expect(await fs.readFile(sourcePath, "utf8")).toBe(originalConfig);
		expect(manager.getConnectionStatus("selected")).toBe("connected");
		expect(getMCPManagerBinding(manager)?.signal.aborted).toBe(false);
		expect(remote.generations).toBe(2);
		expect(remote.calls).toBe(0);
	});

	it("explicit MCP disable is authoritative even with a selected native declaration", async () => {
		let callbacks = 0;
		const opts = await options(api =>
			api.registerMCPSessionBootstrap(() => {
				callbacks++;
				throw new Error("Disabled bootstrap must not execute");
			}),
		);
		const result = await createAgentSession({ ...opts, enableMCP: false });
		sessions.push(result.session);
		expect(callbacks).toBe(0);
		expect(result.mcpManager).toBeUndefined();
	});

	it("selected zero-registration factory fails rather than using ambient discovery", async () => {
		const opts = await options(() => {});
		await expect(createAgentSession(opts)).rejects.toBeInstanceOf(MCPBootstrapError);
	});
});
