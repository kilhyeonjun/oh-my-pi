import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { AuthStorage, type AssistantMessage, type ToolCall } from "@oh-my-pi/pi-ai";
import { getBundledModel } from "@oh-my-pi/pi-catalog/models";
import { AssistantMessageEventStream } from "@oh-my-pi/pi-ai/utils/event-stream";
import { ModelRegistry } from "../src/config/model-registry";
import { Settings } from "../src/config/settings";
import type { ToolCallEvent, ToolCallEventResult, ToolResultEvent } from "../src/extensibility/extensions/types";
import { MCPAdmissionError } from "../src/mcp/errors";
import { createMCPToolName } from "../src/mcp/tool-bridge";
import type { MCPBeforeSendEvent } from "../src/mcp/types";
import { createAgentSession } from "../src/sdk";
import type { AgentSession } from "../src/session/agent-session";
import { SessionManager } from "../src/session/session-manager";

const toolName = createMCPToolName("selected", "send.original");
const sessions: AgentSession[] = [];
const authStores: AuthStorage[] = [];
const dirs: string[] = [];
const servers: Bun.Server<undefined>[] = [];
const gates: Array<() => void> = [];
afterEach(async () => {
	for (const release of gates.splice(0)) release();
	for (const session of sessions.splice(0)) await session.dispose();
	for (const server of servers.splice(0)) server.stop(true);
	for (const auth of authStores.splice(0)) auth.close();
	for (const dir of dirs.splice(0)) await fs.rm(dir, { recursive: true, force: true });
});

async function fixture(options?: {
	direct?: boolean;
	hook?: (event: ToolCallEvent) => ToolCallEventResult | undefined | Promise<ToolCallEventResult | undefined>;
	beforeSend?: (event: MCPBeforeSendEvent) => void | Promise<void>;
}) {
	const bodies: Array<{ name: string; arguments: Record<string, unknown> }> = [];
	const sends: MCPBeforeSendEvent[] = [];
	const calls: ToolCallEvent[] = [];
	const results: ToolResultEvent[] = [];
	const server = Bun.serve({
		port: 0,
		async fetch(request) {
			if (request.method !== "POST") return new Response(null, { status: request.method === "DELETE" ? 204 : 405 });
			const message = (await request.json()) as {
				id?: number;
				method: string;
				params: { name: string; arguments: Record<string, unknown> };
			};
			if (message.id === undefined) return new Response(null, { status: 202 });
			if (message.method === "tools/call") bodies.push(message.params);
			const result =
				message.method === "initialize"
					? {
							protocolVersion: "2025-03-26",
							serverInfo: { name: "mounted-loop", version: "1" },
							capabilities: { tools: {} },
						}
					: message.method === "tools/list"
						? {
								tools: [
									{
										name: "send.original",
										inputSchema: {
											type: "object",
											properties: { text: { type: "string" } },
											required: ["text"],
										},
									},
								],
							}
						: { content: [{ type: "text", text: "physically delivered" }] };
			return Response.json({ jsonrpc: "2.0", id: message.id, result });
		},
	});
	servers.push(server);
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "omp-mcp-mounted-loop-"));
	dirs.push(dir);
	const authStorage = await AuthStorage.create(path.join(dir, "auth.db"));
	authStores.push(authStorage);
	authStorage.keys.setRuntime("openai", "synthetic-key");
	const model = getBundledModel("openai", "gpt-4o-mini");
	const { session } = await createAgentSession({
		cwd: dir,
		agentDir: dir,
		authStorage,
		modelRegistry: new ModelRegistry(authStorage),
		model,
		sessionManager: SessionManager.inMemory(dir),
		settings: Settings.isolated({
			"autolearn.enabled": false,
			"compaction.enabled": false,
			"retry.enabled": false,
			"todo.enabled": false,
		}),
		autoApprove: true,
		toolNames: options?.direct ? [toolName] : ["read", "write", toolName],
		mountedToolNames: options?.direct ? undefined : [toolName],
		skills: [],
		contextFiles: [],
		promptTemplates: [],
		slashCommands: [],
		disableExtensionDiscovery: true,
		enableLsp: false,
		skipPythonPreflight: true,
		mcpBootstrap: { extensionPath: import.meta.path },
		preloadedPreparedExtensions: [
			{
				path: import.meta.path,
				resolvedPath: import.meta.path,
				error: null,
				factory: api => {
					api.registerMCPSessionBootstrap(context => ({
						loadConfigs: async () => ({
							configs: {
								selected: {
									type: "http",
									url: `${server.url}mcp`,
									headerPolicy: "origin-locked",
									timeout: 1000,
								},
							},
							sources: {},
							exaApiKeys: [],
						}),
						admission: () => ({
							signal: context.binding.signal,
							beforeAuth: () => {},
							transportOptions: {
								beforeSend: async event => {
									if (event.association?.kind === "invocation") sends.push(event);
									await options?.beforeSend?.(event);
								},
							},
						}),
						dispose: () => {},
					}));
					api.on("tool_call", async event => {
						calls.push(event);
						return options?.hook?.(event);
					});
					api.on("tool_result", event => {
						results.push(event);
					});
				},
			},
		],
	});
	sessions.push(session);
	let turn = 0;
	const callId = options?.direct ? "direct-native-loop" : "outer-device-loop";
	const toolCall: ToolCall = {
		type: "toolCall",
		id: callId,
		name: options?.direct ? toolName : "write",
		arguments: options?.direct
			? { text: "original payload" }
			: { path: `xd://${toolName}`, content: JSON.stringify({ text: "original payload" }) },
	};
	session.agent.streamFn = () => {
		const first = turn++ === 0;
		const message: AssistantMessage = {
			role: "assistant",
			content: first ? [toolCall] : [{ type: "text", text: "complete" }],
			api: model.api,
			provider: model.provider,
			model: model.id,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: first ? "toolUse" : "stop",
			timestamp: Date.now(),
		};
		const stream = new AssistantMessageEventStream();
		queueMicrotask(() => {
			stream.push({ type: "start", partial: message });
			stream.push({ type: "done", reason: first ? "toolUse" : "stop", message });
		});
		return stream;
	};
	return { session, bodies, sends, calls, results, callId };
}

function invocation(event: MCPBeforeSendEvent | undefined) {
	if (event?.association?.kind !== "invocation") throw new Error("Missing actual native invocation custody");
	return event.association.invocation;
}

describe("actual loop native mounted-device custody", () => {
	for (const direct of [false, true]) {
		it(`${direct ? "direct MCP" : "outer write to mounted MCP"} executes once with canonical native identity and lifecycle`, async () => {
			const f = await fixture({ direct });
			if (!direct) expect(f.session.getXdevToolEntries().some(entry => entry.name === toolName)).toBe(true);
			await f.session.prompt("deliver the payload once");
			expect(f.bodies).toEqual([{ name: "send.original", arguments: { text: "original payload" } }]);
			expect(f.sends).toHaveLength(1);
			const identity = invocation(f.sends[0]);
			expect(identity.toolCallId).toBe(f.callId);
			expect(identity.origin.serverName).toBe("selected");
			expect(identity.origin.toolName).toBe("send.original");
			expect(identity.signal.aborted).toBe(true);
			const nativeCalls = f.calls.filter(event => event.toolName === toolName);
			const nativeResults = f.results.filter(event => event.toolName === toolName);
			expect(nativeCalls).toHaveLength(1);
			expect(nativeCalls[0]?.mcpOrigin).toBe(identity.origin);
			expect(nativeResults).toHaveLength(1);
			expect(nativeResults[0]?.mcpInvocation).toBe(identity);
			expect(nativeResults[0]?.isError).toBe(false);
			expect(f.calls.map(event => event.toolName)).toEqual(direct ? [toolName] : ["write", toolName]);
			const tool = f.session.getToolByName(toolName);
			if (!tool) throw new Error("Missing actual native tool after loop");
			const replay = await tool.execute(f.callId, { text: "replay" }).catch((error: unknown) => error);
			expect(replay).toBeInstanceOf(MCPAdmissionError);
			expect(f.bodies).toHaveLength(1);
		});
	}

	it("native initial-hook denial after outer write produces no physical MCP write", async () => {
		const f = await fixture({
			hook: event =>
				event.toolName === toolName ? { block: true, reason: "native initial hook denied" } : undefined,
		});
		await f.session.prompt("deliver the payload once");
		expect(f.calls.map(event => event.toolName)).toEqual(["write", toolName]);
		expect(f.results.filter(event => event.toolName === "write").map(event => event.isError)).toEqual([true]);
		expect(f.sends).toEqual([]);
		expect(f.bodies).toEqual([]);
	});

	it("cancellation while the native initial hook is held cannot write after release", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		gates.push(release.resolve);
		const f = await fixture({
			hook: async event => {
				if (event.toolName === toolName) {
					entered.resolve();
					await release.promise;
				}
				return undefined;
			},
		});
		const running = f.session.prompt("deliver the payload once").catch((error: unknown) => error);
		await entered.promise;
		const aborted = f.session.abort();
		release.resolve();
		await aborted;
		await running;
		expect(f.calls.map(event => event.toolName)).toEqual(["write", toolName]);
		expect(f.sends).toEqual([]);
		expect(f.bodies).toEqual([]);
	});

	it("terminal native admission denial stays terminal through the mounted route", async () => {
		const f = await fixture({
			beforeSend: event => {
				if (event.association?.kind === "invocation") throw new MCPAdmissionError();
			},
		});
		await f.session.prompt("deliver the payload once");
		expect(f.sends).toHaveLength(1);
		expect(invocation(f.sends[0]).signal.aborted).toBe(true);
		expect(f.bodies).toEqual([]);
		expect(f.results.filter(event => event.toolName === toolName).map(event => event.isError)).toEqual([true]);
		expect(f.results.filter(event => event.toolName === "write").map(event => event.isError)).toEqual([true]);
	});

	it("cancellation at the actual native send gate retains custody and prevents a physical write", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		gates.push(release.resolve);
		const f = await fixture({
			beforeSend: async event => {
				if (event.association?.kind === "invocation") {
					entered.resolve();
					await release.promise;
				}
			},
		});
		const running = f.session.prompt("deliver the payload once").catch((error: unknown) => error);
		await entered.promise;
		const aborted = f.session.abort();
		release.resolve();
		await aborted;
		await running;
		expect(f.sends).toHaveLength(1);
		expect(invocation(f.sends[0]).signal.aborted).toBe(true);
		expect(f.bodies).toEqual([]);
	});
});
