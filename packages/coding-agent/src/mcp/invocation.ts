import { MCPAdmissionError } from "./errors";
import { getMCPManagerBinding, type MCPSessionBinding } from "./session-binding";
import type {
	MCPConfigIdentity,
	MCPControlIdentity,
	MCPInvocationIdentity,
	MCPRequestOptions,
	MCPSendAssociation,
	MCPServerConfig,
	MCPToolDefinition,
	MCPToolOrigin,
} from "./types";

interface ConfigRecord {
	readonly identity: MCPConfigIdentity;
	readonly url: string;
	revision: number;
}
interface ToolRecord {
	readonly tool: object;
	readonly origin: MCPToolOrigin;
	readonly definition: MCPToolDefinition;
	readonly schema: string;
	continuation?: ToolRecord;
	live: boolean;
}
interface Dispatch {
	readonly tool: object;
	readonly signal?: AbortSignal;
	readonly controller: AbortController;
	consumed: boolean;
}
interface InvocationRecord {
	readonly tool: object;
	readonly controller: AbortController;
	live: boolean;
}
export interface MCPSendCustody {
	readonly association: MCPSendAssociation;
	readonly wireId?: string | number;
	readonly body?: string;
	readonly transport: object;
}
interface ExecutionPermit {
	readonly tool: object;
	readonly callId: string;
	readonly dispatch: Dispatch;
	invocation?: MCPInvocationIdentity;
}
interface InvocationRegistry {
	readonly configs: WeakMap<object, ConfigRecord>;
	readonly transports: WeakMap<object, ConfigRecord>;
	readonly tools: WeakMap<object, ToolRecord>;
	readonly dispatches: WeakMap<MCPSessionBinding, Map<string, Dispatch>>;
	readonly permits: WeakMap<object, ExecutionPermit>;
	readonly invocations: WeakMap<MCPInvocationIdentity, InvocationRecord>;
	readonly executionContexts: WeakMap<object, ExecutionPermit>;
	readonly requestOptions: WeakSet<MCPRequestOptions>;
	readonly controls: WeakSet<MCPControlIdentity>;
	readonly custodies: WeakSet<MCPSendCustody>;
}
let registry: InvocationRegistry | undefined;
export function getMCPExecutionInvocation(context: object | undefined): MCPInvocationIdentity | undefined {
	return context && registry?.executionContexts.get(context)?.invocation;
}

export function assertMCPFixedHTTPConfig(config: MCPServerConfig): void {
	if (
		config.type !== "http" ||
		config.headerPolicy !== "origin-locked" ||
		config.auth ||
		config.oauth ||
		config.url.startsWith("!") ||
		Object.values(config.headers ?? {}).some(value => value.startsWith("!"))
	)
		throw new MCPAdmissionError();
	let endpoint: URL;
	try {
		endpoint = new URL(config.url);
	} catch {
		throw new MCPAdmissionError();
	}
	if (
		(endpoint.protocol !== "https:" && endpoint.protocol !== "http:") ||
		endpoint.username ||
		endpoint.password ||
		endpoint.hash
	)
		throw new MCPAdmissionError();
}
export function registerMCPConfig(manager: object, options: object, serverName: string, config: MCPServerConfig): void {
	const owner = getMCPManagerBinding(manager);
	if (!owner) return;
	owner.signal.throwIfAborted();
	assertMCPFixedHTTPConfig(config);
	if (config.type !== "http") throw new MCPAdmissionError();
	registry ??= {
		configs: new WeakMap(),
		transports: new WeakMap(),
		tools: new WeakMap(),
		dispatches: new WeakMap(),
		permits: new WeakMap(),
		invocations: new WeakMap(),
		executionContexts: new WeakMap(),
		requestOptions: new WeakSet(),
		controls: new WeakSet(),
		custodies: new WeakSet(),
	};
	const identity = Object.freeze({ owner, serverName }) as MCPConfigIdentity;
	registry?.configs.set(options, { identity, url: config.url, revision: 0 });
}
export function bindMCPTransport(transport: object, options: object | undefined): void {
	const config = options && registry?.configs.get(options);
	if (config) {
		config.identity.owner.signal.throwIfAborted();
		registry?.transports.set(transport, config);
	}
}
export function isGuardedMCPTransport(transport: object): boolean {
	return registry?.transports.has(transport) ?? false;
}
export function registerMCPTool(tool: object, transport: object, definition: MCPToolDefinition): void {
	const config = registry?.transports.get(transport);
	if (!config) return;
	const origin = Object.freeze({
		config: config.identity,
		serverName: config.identity.serverName,
		toolName: definition.name,
		catalogRevision: ++config.revision,
	}) as MCPToolOrigin;
	registry?.tools.set(tool, { tool, origin, definition, schema: JSON.stringify(definition.inputSchema), live: true });
}
export function linkMCPToolWrapper(wrapper: object, tool: object): void {
	const record = registry?.tools.get(tool);
	if (record) registry?.tools.set(wrapper, record);
}
export function getMCPToolOrigin(tool: unknown): MCPToolOrigin | undefined {
	return typeof tool === "object" && tool !== null ? registry?.tools.get(tool)?.origin : undefined;
}
/** Internal catalog handoff: retired external objects stay retired; only already-running invocations follow it. */
export function continueMCPToolInvocation(tool: object, replacement: object): void {
	const previous = checkedTool(tool);
	const next = checkedTool(replacement);
	if (
		previous &&
		next &&
		previous.origin.config === next.origin.config &&
		previous.origin.catalogRevision < next.origin.catalogRevision &&
		previous.origin.toolName === next.origin.toolName &&
		previous.schema === next.schema
	)
		previous.continuation = next;
}
function checkedInvocationTool(tool: object): ToolRecord | undefined {
	let record = registry?.tools.get(tool);
	while (record?.continuation) {
		if (
			record.definition.name !== record.origin.toolName ||
			JSON.stringify(record.definition.inputSchema) !== record.schema
		)
			throw new MCPAdmissionError();
		record = record.continuation;
	}
	return record && checkedTool(record.tool);
}
export function retireMCPTool(tool: object): void {
	const record = registry?.tools.get(tool);
	if (record) record.live = false;
}
function checkedTool(tool: object): ToolRecord | undefined {
	const record = registry?.tools.get(tool);
	if (
		record &&
		(!record.live ||
			record.origin.config.owner.signal.aborted ||
			record.definition.name !== record.origin.toolName ||
			JSON.stringify(record.definition.inputSchema) !== record.schema)
	)
		throw new MCPAdmissionError();
	return record;
}
export function reserveMCPToolDispatch(tool: object, callId: string, signal?: AbortSignal): void {
	const record = checkedTool(tool);
	if (!record) return;
	signal?.throwIfAborted();
	const owner = record.origin.config.owner;
	let table = registry?.dispatches.get(owner);
	if (!table) {
		table = new Map();
		registry?.dispatches.set(owner, table);
	}
	const previous = table.get(callId);
	if (previous) {
		previous.consumed = true;
		previous.controller.abort(new MCPAdmissionError());
		throw new MCPAdmissionError();
	}
	table.set(callId, { tool: record.tool, signal, controller: new AbortController(), consumed: false });
}
/** Internal loop custody check: an inherited loop marker is not a native reservation. */
export function hasMCPToolDispatch(tool: object, callId: string): boolean {
	const record = registry?.tools.get(tool);
	const dispatch = record && registry?.dispatches.get(record.origin.config.owner)?.get(callId);
	return dispatch !== undefined && !dispatch.consumed && dispatch.tool === record?.tool;
}
export function cancelMCPToolDispatch(tool: object, callId: string): void {
	const origin = getMCPToolOrigin(tool);
	const dispatch = origin && registry?.dispatches.get(origin.config.owner)?.get(callId);
	if (dispatch) {
		dispatch.consumed = true;
		dispatch.controller.abort(new MCPAdmissionError());
	}
}
export function permitMCPToolExecution(tool: object, callId: string, context: object): void {
	const record = checkedTool(tool);
	if (!record) return;
	const dispatch = registry?.dispatches.get(record.origin.config.owner)?.get(callId);
	if (!dispatch || dispatch.consumed || dispatch.tool !== record.tool || registry?.permits.has(context))
		throw new MCPAdmissionError();
	dispatch.signal?.throwIfAborted();
	dispatch.consumed = true;
	const permit = { tool: record.tool, callId, dispatch };
	registry?.permits.set(context, permit);
	registry?.executionContexts.set(context, permit);
}
export function carryMCPExecutionContext(source: object | undefined, target: object): void {
	const permit = source && registry?.permits.get(source);
	if (permit && source) {
		registry?.permits.delete(source);
		registry?.permits.set(target, permit);
		registry?.executionContexts.set(target, permit);
	}
}
export function beginMCPInvocation(
	tool: object,
	callId: string,
	context: object,
	signal?: AbortSignal,
	transport?: object,
): MCPInvocationIdentity | undefined {
	const record = checkedTool(tool);
	if (!record) {
		if (transport && registry?.transports.has(transport)) throw new MCPAdmissionError();
		return undefined;
	}
	const permit = registry?.permits.get(context);
	registry?.permits.delete(context);
	if (!permit || permit.tool !== record.tool || permit.callId !== callId) throw new MCPAdmissionError();
	const controller = new AbortController();
	const signals = [record.origin.config.owner.signal, controller.signal, permit.dispatch.controller.signal];
	if (signal) signals.push(signal);
	if (permit.dispatch.signal) signals.push(permit.dispatch.signal);
	const lifetime = AbortSignal.any(signals);
	lifetime.throwIfAborted();
	const identity = Object.freeze({
		origin: record.origin,
		toolCallId: callId,
		signal: lifetime,
	}) as MCPInvocationIdentity;
	registry?.invocations.set(identity, { tool: record.tool, controller, live: true });
	permit.invocation = identity;
	return identity;
}
export function finishMCPInvocation(identity: MCPInvocationIdentity | undefined): void {
	if (!identity) return;
	const record = registry?.invocations.get(identity);
	if (record) {
		record.live = false;
		record.controller.abort();
	}
}
export function createMCPInvocationOptions(invocation: MCPInvocationIdentity, signal?: AbortSignal): MCPRequestOptions {
	const options = { invocation, signal: signal ? AbortSignal.any([signal, invocation.signal]) : invocation.signal };
	registry?.requestOptions.add(options);
	return options;
}
export function createMCPRequestCustody(
	transport: object,
	method: string,
	options: MCPRequestOptions | undefined,
	wireId: string | number,
	body: string,
): MCPSendCustody | undefined {
	const config = registry?.transports.get(transport);
	if (!config) return undefined;
	config.identity.owner.signal.throwIfAborted();
	if (method === "tools/call") {
		const invocation = options?.invocation;
		const record = invocation && registry?.invocations.get(invocation);
		if (
			!options ||
			!registry?.requestOptions.has(options) ||
			!invocation ||
			!record?.live ||
			invocation.origin.config !== config.identity ||
			!checkedInvocationTool(record.tool)
		)
			throw new MCPAdmissionError();
		invocation.signal.throwIfAborted();
		return sealCustody({ association: Object.freeze({ kind: "invocation", invocation }), wireId, body, transport });
	}
	if (options?.invocation || (method !== "initialize" && method !== "tools/list" && method !== "ping"))
		throw new MCPAdmissionError();
	return createMCPControlCustody(
		transport,
		method === "initialize" ? "startup" : method === "tools/list" ? "catalog" : "maintenance",
		wireId,
		body,
	);
}
function sealCustody(custody: MCPSendCustody): MCPSendCustody {
	Object.freeze(custody);
	registry?.custodies.add(custody);
	return custody;
}
export function createMCPControlCustody(
	transport: object,
	phase: MCPControlIdentity["phase"],
	wireId?: string | number,
	body?: string,
): MCPSendCustody | undefined {
	const config = registry?.transports.get(transport);
	if (!config) return undefined;
	config.identity.owner.signal.throwIfAborted();
	const control = Object.freeze({
		config: config.identity,
		phase,
		signal: config.identity.owner.signal,
	}) as MCPControlIdentity;
	registry?.controls.add(control);
	return sealCustody({ association: Object.freeze({ kind: "control", control }), wireId, body, transport });
}
export function checkMCPSendCustody(custody: MCPSendCustody, url: string, body: string | undefined): void {
	const config = registry?.transports.get(custody.transport);
	if (!registry?.custodies.has(custody) || !config || config.url !== url || custody.body !== body)
		throw new MCPAdmissionError();
	config.identity.owner.signal.throwIfAborted();
	const association = custody.association;
	if (association.kind === "invocation") {
		const record = registry?.invocations.get(association.invocation);
		if (!record?.live || association.invocation.origin.config !== config.identity) throw new MCPAdmissionError();
		checkedInvocationTool(record.tool);
		association.invocation.signal.throwIfAborted();
	} else if (!registry?.controls.has(association.control) || association.control.config !== config.identity)
		throw new MCPAdmissionError();
}
