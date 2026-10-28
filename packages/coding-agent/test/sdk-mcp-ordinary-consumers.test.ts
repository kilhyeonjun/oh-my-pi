import { afterEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { AuthStorage } from "@oh-my-pi/pi-ai";
import { getBundledModel } from "@oh-my-pi/pi-catalog/models";
import { getAgentDir, getMCPConfigPath, setAgentDir, TempDir } from "@oh-my-pi/pi-utils";
import { initTheme } from "@oh-my-pi/pi-tui/theme";
import { ModelRegistry } from "../src/config/model-registry";
import { Settings } from "../src/config/settings";
import { readMCPConfigFile } from "../src/mcp/config-writer";
import { createMCPToolName } from "../src/mcp/tool-bridge";
import { createExtensionDashboardRuntime } from "../src/modes/components/extensions/dashboard-runtime";
import { MCPCommandController } from "../src/modes/controllers/mcp-command-controller";
import { createAgentSession, type CreateAgentSessionOptions } from "../src/sdk";
import type { AgentSession } from "../src/session/agent-session";
import { SessionManager } from "../src/session/session-manager";
import { createMCPProxyTools, createSubagentSettings, followMCPTools } from "../src/task/executor";
import { createInteractiveModeContext } from "./helpers/interactive-mode-context";

const sessions: AgentSession[] = [];
afterEach(async () => {
	for (const session of sessions.splice(0).reverse()) await session.dispose();
});

function catalog() {
	let generations = 0;
	let calls = 0;
	const server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		async fetch(request) {
			if (request.method === "DELETE") return new Response(null, { status: 204 });
			if (request.method !== "POST") return new Response(null, { status: 405 });
			const message = (await request.json()) as { id?: number; method: string };
			if (message.id === undefined) return new Response(null, { status: 202 });
			let result: unknown;
			if (message.method === "initialize") {
				generations++;
				result = {
					protocolVersion: "2025-03-26",
					serverInfo: { name: "ordinary-sdk-fixture", version: "1" },
					capabilities: { tools: {} },
				};
			} else if (message.method === "tools/list") {
				result = {
					tools: [
						{
							name: "counter",
							description: "Increment counter",
							inputSchema: { type: "object", properties: {} },
						},
					],
				};
			} else if (message.method === "tools/call") {
				calls++;
				result = { content: [{ type: "text", text: String(calls) }] };
			} else {
				return Response.json({
					jsonrpc: "2.0",
					id: message.id,
					error: { code: -32601, message: "Unsupported fixture method" },
				});
			}
			return Response.json({ jsonrpc: "2.0", id: message.id, result });
		},
	});
	return {
		server,
		config: { type: "http", url: `${server.url}mcp`, timeout: 1000 },
		get generations() {
			return generations;
		},
		get calls() {
			return calls;
		},
	};
}

async function setup(cwd: string, authStorage: AuthStorage): Promise<CreateAgentSessionOptions> {
	return {
		cwd,
		agentDir: cwd,
		authStorage,
		modelRegistry: new ModelRegistry(authStorage),
		model: getBundledModel("openai", "gpt-4o-mini"),
		sessionManager: SessionManager.inMemory(cwd),
		settings: Settings.isolated({
			"tools.xdev": false,
			"autolearn.enabled": false,
			"marketplace.autoUpdate": "off",
			"mcp.startupTimeoutMs": 0,
		}),
		skills: [],
		contextFiles: [],
		promptTemplates: [],
		slashCommands: [],
		disableExtensionDiscovery: true,
		enableLsp: false,
		skipPythonPreflight: true,
	};
}

const toolName = createMCPToolName("ordinary", "counter");

describe("ordinary SDK MCP consumers", () => {
	it("reconnects through the command and disables the same live manager through the dashboard", async () => {
		using dir = TempDir.createSync("omp-sdk-mcp-consumers-");
		const cwd = dir.path();
		const oldAgentDir = getAgentDir();
		const home = spyOn(os, "homedir").mockReturnValue(cwd);
		const remote = catalog();
		const auth = await AuthStorage.create(":memory:");
		try {
			setAgentDir(cwd);
			await fs.mkdir(path.dirname(getMCPConfigPath("project", cwd)), { recursive: true });
			await Bun.write(getMCPConfigPath("project", cwd), JSON.stringify({ mcpServers: { ordinary: remote.config } }));
			const options = await setup(cwd, auth);
			const created = await createAgentSession(options);
			const { session } = created;
			sessions.push(session);
			const manager = session.getNativeMCPManager();
			if (!manager) throw new Error("Ordinary session lost its live MCP manager");
			expect(created.mcpManager).toBe(manager);
			expect(manager.getConnectedServers()).toContain("ordinary");
			expect(session.getToolByName(toolName)).toBeDefined();
			expect(remote.generations).toBe(1);
			initTheme();
			const errors: string[] = [];
			const controller = new MCPCommandController(
				createInteractiveModeContext({
					session,
					showError: message => {
						errors.push(message);
					},
				}),
			);
			await controller.handle("/mcp reconnect ordinary");
			expect(errors).toEqual([]);
			expect(remote.generations).toBe(2);
			const tool = session.getToolByName(toolName);
			if (!tool) throw new Error("Reconnect removed the ordinary tool");
			expect((await tool.execute("ordinary-after-reconnect", {})).content).toEqual([{ type: "text", text: "1" }]);
			expect(remote.calls).toBe(1);
			const dashboard = createExtensionDashboardRuntime({
				cwd,
				settings: session.settings,
				getMcpManager: () => session.getNativeMCPManager(),
				onMcpToolsChanged: tools => session.refreshMCPTools(tools),
			});
			await dashboard.persistMcpToggle("ordinary", false);
			await dashboard.applyMcpToggle("ordinary", false);
			expect((await readMCPConfigFile(getMCPConfigPath("project", cwd))).mcpServers?.ordinary?.enabled).toBe(false);
			expect(manager.getConnectedServers()).not.toContain("ordinary");
			expect(manager.getTools()).toEqual([]);
			expect(session.getToolByName(toolName)).toBeUndefined();
			expect(remote.calls).toBe(1);
			await session.dispose();
			sessions.splice(sessions.indexOf(session), 1);
		} finally {
			for (const session of sessions.splice(0).reverse()) await session.dispose();
			remote.server.stop(true);
			auth.close();
			home.mockRestore();
			setAgentDir(oldAgentDir);
		}
	}, 30_000);

	it("preserves borrowed ownership and executes initial tan proxies with discovery disabled and a stable catalog", async () => {
		using dir = TempDir.createSync("omp-sdk-mcp-proxies-");
		const cwd = dir.path();
		const oldAgentDir = getAgentDir();
		const home = spyOn(os, "homedir").mockReturnValue(cwd);
		const remote = catalog();
		const auth = await AuthStorage.create(":memory:");
		try {
			setAgentDir(cwd);
			await fs.mkdir(path.dirname(getMCPConfigPath("project", cwd)), { recursive: true });
			await Bun.write(getMCPConfigPath("project", cwd), JSON.stringify({ mcpServers: { ordinary: remote.config } }));
			const options = await setup(cwd, auth);
			const { session: parent } = await createAgentSession(options);
			sessions.push(parent);
			const manager = parent.getNativeMCPManager();
			if (!manager) throw new Error("Missing ordinary parent manager");
			const { session: borrowed } = await createAgentSession({
				...options,
				sessionManager: SessionManager.inMemory(cwd),
				mcpManager: manager,
				parentTaskPrefix: "borrowed",
			});
			sessions.push(borrowed);
			expect(borrowed.getNativeMCPManager()).toBe(manager);
			await borrowed.dispose();
			sessions.splice(sessions.indexOf(borrowed), 1);
			expect(manager.getConnectedServers()).toContain("ordinary");
			const follower = followMCPTools(manager);
			try {
				const { session: child } = await createAgentSession({
					...options,
					settings: createSubagentSettings(parent.settings),
					sessionManager: SessionManager.inMemory(cwd),
					enableMCP: false,
					mcpTools: createMCPProxyTools(manager),
					toolNames: parent.getEnabledToolNames(),
					mountedToolNames: parent.getMountedXdevToolNames(),
					parentTaskPrefix: "Tan-ordinary",
				});
				sessions.push(child);
				follower.bind(child);
				expect(child.getNativeMCPManager()).toBeUndefined();
				expect(child.getActiveToolNames()).toContain(toolName);
				const proxy = child.getToolByName(toolName);
				if (!proxy) throw new Error("Initial tan proxy was dropped");
				expect((await proxy.execute("tan-initial-proxy", {})).content).toEqual([{ type: "text", text: "1" }]);
				expect(remote.calls).toBe(1);
				expect(remote.generations).toBe(1);
				await child.dispose();
				sessions.splice(sessions.indexOf(child), 1);
			} finally {
				follower.dispose();
			}
			expect(manager.getConnectedServers()).toContain("ordinary");
			await parent.dispose();
			sessions.splice(sessions.indexOf(parent), 1);
			expect(manager.getConnectedServers()).toEqual([]);
		} finally {
			for (const session of sessions.splice(0).reverse()) await session.dispose();
			remote.server.stop(true);
			auth.close();
			home.mockRestore();
			setAgentDir(oldAgentDir);
		}
	}, 30_000);
});
