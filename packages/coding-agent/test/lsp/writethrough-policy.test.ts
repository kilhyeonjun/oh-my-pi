import { afterEach, describe, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { FileFormatResult } from "@oh-my-pi/pi-tui/tools/lsp";
import { configCache } from "../../src/lsp/config";
import { getDiagnosticsForFile } from "../../src/lsp/diagnostics";
import type { LspPolicy, ServerConfig } from "../../src/lsp/types";
import { createLspWritethrough } from "../../src/lsp/writethrough";

const roots: string[] = [];
afterEach(() => {
	for (const root of roots.splice(0)) {
		configCache.delete(root);
		fs.rmSync(root, { recursive: true, force: true });
	}
});

function workspace(): { cwd: string; dst: string } {
	const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "omp-writethrough-policy-"));
	roots.push(cwd);
	return { cwd, dst: path.join(cwd, "source.ts") };
}

describe("writethrough policy", () => {
	for (const batched of [false, true]) {
		test(`denied formatting commits only tool content${batched ? " in a batch" : ""}`, async () => {
			const { cwd, dst } = workspace();
			const content = "const value=1;\n";
			const requests: Parameters<LspPolicy["beforeEdit"]>[0][] = [];
			let factoryCalls = 0;
			const policy: LspPolicy = {
				scope: `format-denied-${batched}`,
				async beforeExecute() {
					throw new Error("unexpected process");
				},
				async beforeEdit(request) {
					requests.push(request);
					throw new Error("format denied");
				},
			};
			const server: ServerConfig = {
				command: "policy-formatter",
				fileTypes: [".ts"],
				rootMarkers: [],
				isLinter: true,
				createClient() {
					factoryCalls++;
					throw new Error("denied formatter must never be constructed");
				},
			};
			configCache.set(cwd, { servers: { formatter: server } });
			const write = createLspWritethrough(cwd, { enableFormat: true, policy });
			const result = await write(
				dst,
				content,
				undefined,
				undefined,
				batched ? { id: `format-policy-${cwd}`, flush: true } : undefined,
			);
			expect(result.finalContent).toBe(content);
			expect(fs.readFileSync(dst, "utf8")).toBe(content);
			expect(result.diagnostics?.formatter).toBe(FileFormatResult.FAILED);
			expect(factoryCalls).toBe(0);
			expect(requests).toEqual([{ origin: "format", cwd, changes: [{ kind: "edit", path: dst }] }]);
		});
	}

	test("approved formatting and subsequent diagnostics retain the same policy", async () => {
		const { cwd, dst } = workspace();
		const policy: LspPolicy = {
			scope: `format-approved-${cwd}`,
			async beforeExecute() {},
			async beforeEdit() {},
		};
		const receivedPolicies: Array<LspPolicy | undefined> = [];
		let lintCalls = 0;
		const server: ServerConfig = {
			command: "policy-formatter",
			fileTypes: [".ts"],
			rootMarkers: [],
			isLinter: true,
			createClient(_config, _cwd, receivedPolicy) {
				receivedPolicies.push(receivedPolicy);
				return {
					async format(_file, content) {
						return content.replace("value=1", "value = 1");
					},
					async lint() {
						lintCalls++;
						return [];
					},
				};
			},
		};
		configCache.set(cwd, { servers: { formatter: server } });
		const result = await createLspWritethrough(cwd, { enableFormat: true, policy })(dst, "const value=1;\n");
		expect(result.finalContent).toBe("const value = 1;\n");
		expect(fs.readFileSync(dst, "utf8")).toBe(result.finalContent);
		expect(result.diagnostics?.formatter).toBe(FileFormatResult.FORMATTED);
		await getDiagnosticsForFile(dst, cwd, [["formatter", server]], { policy });
		expect(receivedPolicies).toEqual([policy]);
		expect(lintCalls).toBe(1);
	});
});
