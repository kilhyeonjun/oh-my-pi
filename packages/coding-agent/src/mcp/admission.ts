import { untilAborted } from "@oh-my-pi/pi-utils";
import { AbortError } from "@oh-my-pi/pi-utils/abortable";
import { isMCPAdmissionError, MCPAdmissionError } from "./errors";

/** Only a current transport-facing continuation may perform the effect. */
export async function runMCPAdmission(signal: AbortSignal, callback: () => void | Promise<void>): Promise<void> {
	try {
		await untilAborted(signal, async () => {
			let result: unknown;
			try {
				result = await callback();
			} catch {
				// Never retain callback diagnostics, causes, headers or environment values.
				throw new MCPAdmissionError();
			}
			if (result !== undefined) throw new MCPAdmissionError();
		});
	} catch (error) {
		// A manager retires its source binding before its throwing callback settles.
		if (signal.aborted && isMCPAdmissionError(signal.reason)) throw signal.reason;
		throw error;
	}
	if (signal.aborted) throw isMCPAdmissionError(signal.reason) ? signal.reason : new AbortError(signal);
}
