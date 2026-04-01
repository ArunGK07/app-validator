import path from "node:path";
import { runNativeValidation } from "./validation/engine.mjs";
const root = "d:/Turing/Projects/workspace/task-output";
for (const taskId of [15780,15784,15791,15790,15792]) {
  const taskDir = path.join(root, String(taskId));
  const logFile = path.join(taskDir, "_logs", `tmp_validate_${taskId}_latest.log`);
  const result = await runNativeValidation(taskId, taskDir, logFile);
  const summary = result?.summary ?? {};
  console.log(JSON.stringify({taskId, summary}, null, 2));
}
