import type { ExtensionDashboardRuntime } from "@oh-my-pi/pi-tui/overlays/extensions/extension-dashboard";
import { getMCPConfigPath } from "@oh-my-pi/pi-utils";
import { parseRuleAgents, parseRuleConditionAndScope } from "../../../capability/rule";
import type { Settings } from "../../../config/settings";
import { getAllProvidersInfo, isForeignUserProvider, isUserSourceEnabled } from "../../../discovery";
import type { CustomTool } from "../../../extensibility/custom-tools/types";
import { setMcpServerEnabled } from "../../../mcp/config-writer";
import type { MCPManager } from "../../../mcp/manager";
import { MCPBootstrapError } from "../../../mcp/bootstrap";
import { getMCPManagerBinding } from "../../../mcp/session-binding";
import { MCP_CONNECTION_STATUS_EVENT_CHANNEL } from "../../../mcp/startup-events";
import type { EventBus } from "../../../utils/event-bus";
import { toolFileHeaderDescription } from "./inspector-runtime";
import { applyMcpToggleRuntime } from "./mcp-runtime";
import { loadAllExtensions, toggleProvider, toggleUserSource } from "./state-manager";

import { cfgDisabledExtensions } from "../../../extensibility/settings";
import { cfgMcpEnableProjectConfig } from "../../../mcp/settings";

/** Bind the dashboard's display-only contract to the live application. */
export function createExtensionDashboardRuntime(options: {
	cwd: string;
	settings: Settings;
	getMcpManager: () => MCPManager | undefined;
	eventBus?: EventBus;
	onMcpToolsChanged?: (tools: CustomTool[]) => Promise<void> | void;
	browserMcpFilterEnabled?: () => boolean;
}): ExtensionDashboardRuntime {
	const { cwd, settings, getMcpManager, eventBus, onMcpToolsChanged, browserMcpFilterEnabled } = options;
	const listeners = new Set<(manager: MCPManager | undefined) => void>();
	const liveManager = () => {
		const manager = getMcpManager();
		for (const listener of listeners) listener(manager);
		return manager;
	};
	return {
		getDisabledExtensions: () => cfgDisabledExtensions.get(settings),
		setDisabledExtensions: ids => cfgDisabledExtensions.set(settings, ids),
		getProviders: () =>
			getAllProvidersInfo().map(provider => ({
				...provider,
				userSourceEnabled: isUserSourceEnabled(provider.id),
				foreignUserSource: isForeignUserProvider(provider.id),
			})),
		loadExtensions: disabledIds => loadAllExtensions(cwd, disabledIds),
		toggleProvider,
		toggleUserSource,
		async persistMcpToggle(name, enabled, sourcePath) {
			if (getMCPManagerBinding(getMcpManager()))
				throw new MCPBootstrapError("conflict", "This MCP session has a sealed native source selection");
			await setMcpServerEnabled({
				userPath: getMCPConfigPath("user", cwd),
				projectPath: getMCPConfigPath("project", cwd),
				sourcePath,
				name,
				enabled,
			});
		},
		applyMcpToggle: (name, enabled) =>
			applyMcpToggleRuntime({
				name,
				enabled,
				cwd,
				manager: getMcpManager(),
				session: onMcpToolsChanged ? { refreshMCPTools: onMcpToolsChanged } : undefined,
				discovery: {
					enableProjectConfig: cfgMcpEnableProjectConfig.get(settings),
					filterExa: true,
					filterBrowser: browserMcpFilterEnabled?.() ?? false,
				},
				onStatus: event => eventBus?.emit(MCP_CONNECTION_STATUS_EVENT_CHANNEL, event),
			}),
		subscribeMcpChanges(onChange) {
			let current: MCPManager | undefined;
			let subscriptions: Array<() => void> = [];
			const bind = (manager: MCPManager | undefined) => {
				if (current === manager) return;
				for (const unsubscribe of subscriptions) unsubscribe();
				current = manager;
				subscriptions = manager
					? [
							manager.addNotificationListener(onChange),
							manager.addConnectionStatusListener(onChange),
							manager.addCatalogChangeListener(onChange),
						]
					: [];
			};
			listeners.add(bind);
			bind(getMcpManager());
			const unsubscribeStatus = eventBus?.on(MCP_CONNECTION_STATUS_EVENT_CHANNEL, () => {
				bind(getMcpManager());
				onChange();
			});
			return [
				() => {
					listeners.delete(bind);
					unsubscribeStatus?.();
					for (const unsubscribe of subscriptions) unsubscribe();
				},
			];
		},
		mcpSource: {
			getConnectionStatus: name => liveManager()?.getConnectionStatus(name) ?? "disconnected",
			getConnection: name => liveManager()?.getConnection(name),
			getTools: () => liveManager()?.getTools() ?? [],
			getServerResources: name => liveManager()?.getServerResources(name),
			getServerPrompts: name => liveManager()?.getServerPrompts(name),
		},
		inspectorSource: {
			readToolHeader: toolFileHeaderDescription,
			parseRule: raw => ({ ...parseRuleConditionAndScope(raw), agents: parseRuleAgents(raw.agents) }),
		},
	};
}
