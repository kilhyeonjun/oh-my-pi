/** Exact transport identity, independent of normalized names and public tool fields. */
export interface MCPToolOrigin {
	readonly mcpServerName: string;
	readonly mcpToolName: string;
}

const originKey = Symbol("native MCP origin");
type OriginTool = { readonly [originKey]?: MCPToolOrigin };

/** Read native provenance without trusting public metadata or model arguments. */
export function getMCPToolOrigin(tool: object): MCPToolOrigin | undefined {
	return (tool as OriginTool)[originKey];
}

/** @internal Only native MCP constructors may attest a transport identity. */
export function attestMCPToolOrigin(tool: object, serverName: string, toolName: string): void {
	Object.defineProperty(tool, originKey, {
		value: Object.freeze({ mcpServerName: serverName, mcpToolName: toolName }),
	});
}

/** Preserve an existing native attestation across adapters; never infer one. */
export function inheritMCPToolOrigin(source: object, target: object): void {
	const origin = getMCPToolOrigin(source);
	if (origin) Object.defineProperty(target, originKey, { value: origin });
}
