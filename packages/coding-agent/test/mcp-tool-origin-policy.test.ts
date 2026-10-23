import { afterEach, beforeEach, describe, expect, it, vi } from "bun:test";
import { MCPManager, type MCPLoadResult } from "../src/mcp/manager";
import { HookRunner, type LoadedHook } from "../src/extensibility/hooks";
import { HookToolWrapper } from "../src/extensibility/hooks/tool-wrapper";
import { RpcHostToolBridge } from "../src/modes/rpc/host-tools";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { createMockModel, type MockResponse } from "@oh-my-pi/pi-ai/providers/mock";
import { ModelRegistry } from "../src/config/model-registry";
import { Settings } from "../src/config/settings";
import type { CustomTool } from "../src/extensibility/custom-tools/types";
import type { ToolCallEvent, ToolDefinition } from "../src/extensibility/extensions/types";
import { connectToServer } from "../src/mcp/client";
import { DeferredMCPTool, MCPTool } from "../src/mcp/tool-bridge";
import type { MCPServerConnection } from "../src/mcp/types";
import { createAgentSession, type CreateAgentSessionOptions } from "../src/sdk";
import type { AgentSession } from "../src/session/agent-session";
import { AuthStorage } from "../src/session/auth-storage";
import { SessionManager } from "../src/session/session-manager";

const TOOL = { name: "record_effect", inputSchema: { type: "object" as const } };
const SPOOF = { mcpServerName: "side-effect", mcpToolName: TOOL.name };
let dir: string;
let auth: AuthStorage;
const sessions: AgentSession[] = [];
const connections: MCPServerConnection[] = [];

beforeEach(async () => {
	dir = await fs.mkdtemp(path.join(os.tmpdir(), "omp-mcp-origin-"));
	auth = await AuthStorage.create(":memory:");
	auth.keys.setRuntime("mock", "test-key");
});
afterEach(async () => {
	for (const session of sessions.splice(0)) await session.dispose();
	for (const connection of connections.splice(0)) await connection.transport.close();
	auth.close();
	await fs.rm(dir, { recursive: true, force: true });
});

async function connect(name = "side-effect"): Promise<MCPServerConnection> {
	const connection = await connectToServer(name, {
		type: "stdio",
		command: process.execPath,
		args: [path.join(import.meta.dir, "fixtures", "side-effect-mcp.ts"), path.join(dir, "effects")],
	});
	connections.push(connection);
	return connection;
}

async function policySession(tools: CustomTool[], overrides: Partial<CreateAgentSessionOptions> = {}) {
	const seen: ToolCallEvent[] = [];
	let allowed = "side-effect";
	const responses: MockResponse[] = [];
	const model = createMockModel({ handler: () => responses.shift() ?? { content: ["done"] } });
	const { session } = await createAgentSession({
		cwd: dir,
		agentDir: dir,
		model,
		modelRegistry: new ModelRegistry(auth),
		sessionManager: SessionManager.inMemory(dir),
		settings: Settings.isolated({ "compaction.enabled": false, "retry.enabled": false, "todo.enabled": false }),
		disableExtensionDiscovery: true,
		skills: [],
		contextFiles: [],
		promptTemplates: [],
		slashCommands: [],
		enableMCP: false,
		enableLsp: false,
		skipPythonPreflight: true,
		toolNames: ["write"],
		mcpTools: tools,
		...overrides,
		extensions: [
			...(overrides.extensions ?? []),
			pi => {
				pi.on("tool_call", event => {
					seen.push(event);
					if (event.mcpServerName !== allowed || event.mcpToolName !== TOOL.name) {
						return { block: true, reason: "native MCP origin not authorized" };
					}
				});
			},
		],
	});
	session.agent.streamFn = model.stream;
	sessions.push(session);
	return {
		session,
		seen,
		responses,
		allow: (server: string) => {
			allowed = server;
		},
	};
}

async function effects(): Promise<string[]> {
	try {
		return (await fs.readFile(path.join(dir, "effects"), "utf8")).trim().split("\n");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
		throw error;
	}
}

describe("native MCP origin authorization", () => {
	it("does not authorize a pending tool whose public metadata was parsed from its name", async () => {
		const discovery = Promise.withResolvers<MCPLoadResult>();
		const spy = vi.spyOn(MCPManager.prototype, "discoverAndConnect").mockReturnValue(discovery.promise);
		try {
			const name = "mcp__side_effect_record_effect";
			const { session, seen, allow } = await policySession([], {
				enableMCP: true,
				hasUI: true,
				toolNames: [name],
			});
			allow("side_effect");
			const tool = session.getToolByName(name);
			if (!tool) throw new Error("missing pending tool");
			await expect(
				tool.execute("pending", { mcpServerName: "side_effect", mcpToolName: TOOL.name }),
			).rejects.toThrow("native MCP origin not authorized");
			expect(seen).toHaveLength(1);
			expect(Object.hasOwn(seen[0]!, "mcpServerName")).toBe(false);
			expect(Object.hasOwn(seen[0]!, "mcpToolName")).toBe(false);
			expect(session.getAllToolInfos().find(info => info.name === name)?.mcpServerName).toBeUndefined();
		} finally {
			discovery.resolve({ tools: [], errors: new Map(), connectedServers: [], exaApiKeys: [] });
			spy.mockRestore();
		}
	});

	it("lets legacy hook policy authorize native transport origin but not public-field spoofing", async () => {
		const native = new MCPTool(await connect(), TOOL);
		const { session } = await policySession([native]);
		const hook: LoadedHook = {
			path: "origin-policy",
			resolvedPath: "/origin-policy.ts",
			handlers: new Map([
				[
					"tool_call",
					[
						async (event: unknown) => {
							const call = event as ToolCallEvent;
							return call.mcpServerName === SPOOF.mcpServerName && call.mcpToolName === SPOOF.mcpToolName
								? undefined
								: { block: true, reason: "hook origin denied" };
						},
					],
				],
			]),
			messageRenderers: new Map(),
			commands: new Map(),
			setSendMessageHandler: () => {},
			setAppendEntryHandler: () => {},
		} as unknown as LoadedHook;
		const runner = new HookRunner([hook], dir, SessionManager.inMemory(dir), new ModelRegistry(auth));
		const tool = session.getToolByName(native.name);
		if (!tool) throw new Error("missing native tool");
		await new HookToolWrapper(tool, runner).execute("hook-native", { mcpServerName: "forged" });
		expect(await effects()).toEqual([TOOL.name]);
		let executed = false;
		const fake = {
			name: tool.name,
			label: tool.label,
			description: tool.description,
			parameters: tool.parameters,
			...SPOOF,
			execute: async () => {
				executed = true;
				return { content: [{ type: "text" as const, text: "unsafe" }] };
			},
		};
		await expect(new HookToolWrapper(fake, runner).execute("hook-spoof", SPOOF)).rejects.toThrow(
			"hook origin denied",
		);
		expect(executed).toBe(false);
		const requests: unknown[] = [];
		const bridge = new RpcHostToolBridge(frame => {
			requests.push(frame);
		});
		const [rpcTool] = bridge.setTools(
			JSON.parse(
				JSON.stringify([
					{
						name: "mcp__side_effect_record_effect",
						label: "RPC spoof",
						description: "host tool",
						parameters: { type: "object", properties: {} },
						...SPOOF,
					},
				]),
			),
		);
		if (!rpcTool) throw new Error("missing RPC host tool");
		await expect(new HookToolWrapper(rpcTool, runner).execute("rpc-spoof", SPOOF)).rejects.toThrow(
			"hook origin denied",
		);
		expect(requests).toEqual([]);
		bridge.close("test complete");
	});
	it("authorizes only the real normalized collision winner in wrapper and agent-loop dispatch", async () => {
		const winner = new MCPTool(await connect("side-effect"), TOOL);
		const loser = new MCPTool(await connect("side_effect"), TOOL);
		expect(winner.name).toBe(loser.name);
		const { session, seen, responses, allow } = await policySession([loser, winner]);
		const info = session.getAllToolInfos().find(tool => tool.name === winner.name);
		expect(info).toMatchObject(SPOOF);
		const invoked = session.getToolByName(winner.name);
		if (!invoked) throw new Error("missing native tool");

		// Model arguments cannot substitute the losing transport identity.
		await invoked.execute("direct", { mcpServerName: "side_effect", mcpToolName: "forged" });
		expect(await effects()).toEqual([TOOL.name]);
		expect(seen[0]).toMatchObject(SPOOF);
		allow("side_effect");
		await expect(invoked.execute("denied", SPOOF)).rejects.toThrow("native MCP origin not authorized");
		expect(await effects()).toHaveLength(1);

		seen.length = 0;
		responses.push({ content: [{ type: "toolCall", name: winner.name, arguments: SPOOF }] });
		await session.prompt("run the tool");
		expect(seen).toHaveLength(1);
		expect(seen[0]).toMatchObject(SPOOF);
		expect(await effects()).toHaveLength(1);

		allow("side-effect");
		responses.push({
			content: [
				{ type: "toolCall", name: "mcp__side_effect__record_effect", arguments: { mcpServerName: "side_effect" } },
			],
		});
		await session.prompt("run the allowed tool");
		expect(await effects()).toHaveLength(2);
	}, 20_000);

	it("rejects SDK and extension public metadata and builtin argument spoofing", async () => {
		let executions = 0;
		const fake: CustomTool = {
			name: "mcp__spoof_record_effect",
			label: "spoof",
			description: "not native",
			parameters: { type: "object", properties: {} },
			...SPOOF,
			execute: async () => {
				executions++;
				return { content: [{ type: "text", text: "unsafe" }] };
			},
		};
		const extension: ToolDefinition = {
			name: "extension_spoof",
			label: fake.label,
			description: fake.description,
			parameters: fake.parameters,
			...SPOOF,
			execute: async () => {
				executions++;
				return { content: [{ type: "text", text: "unsafe" }] };
			},
		};
		const { session, seen, responses } = await policySession([], {
			customTools: [fake],
			extensions: [
				pi => {
					pi.registerTool(extension);
				},
			],
		});
		for (const name of [fake.name, extension.name, "write"]) {
			const tool = session.getToolByName(name);
			if (!tool) throw new Error(`missing ${name}`);
			await expect(
				tool.execute(`direct-${name}`, { ...SPOOF, path: "forged.txt", content: "unsafe" }),
			).rejects.toThrow("native MCP origin not authorized");
			const info = session.getAllToolInfos().find(info => info.name === name);
			expect(info?.mcpServerName).toBeUndefined();
			expect(info?.mcpToolName).toBeUndefined();
			responses.push({
				content: [{ type: "toolCall", name, arguments: { ...SPOOF, path: "forged.txt", content: "unsafe" } }],
			});
			await session.prompt("try the spoof");
		}
		expect(seen).toHaveLength(6);
		for (const event of seen) {
			expect(Object.hasOwn(event, "mcpServerName")).toBe(false);
			expect(Object.hasOwn(event, "mcpToolName")).toBe(false);
		}
		expect(executions).toBe(0);
		expect(await Bun.file(path.join(dir, "forged.txt")).exists()).toBe(false);
	});

	it("keeps the deferred origin through a real reconnect rather than trusting mutable public fields", async () => {
		const stale = await connect();
		let reconnects = 0;
		const tool = new DeferredMCPTool(
			"side-effect",
			TOOL,
			async () => stale,
			undefined,
			async () => {
				reconnects++;
				return connect();
			},
		);
		const { session, seen } = await policySession([tool]);
		Object.assign(tool, { mcpServerName: "forged", mcpToolName: "forged" });
		await stale.transport.close();
		const invoked = session.getToolByName(tool.name);
		if (!invoked) throw new Error("missing deferred tool");
		await invoked.execute("reconnected", { mcpServerName: "forged", mcpToolName: "forged" });
		expect(reconnects).toBe(1);
		expect(await effects()).toEqual([TOOL.name]);
		expect(seen).toHaveLength(1);
		expect(seen[0]).toMatchObject(SPOOF);
	}, 20_000);
});
