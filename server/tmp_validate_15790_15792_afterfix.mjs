import path from "node:path";
import { runNativeValidation } from "./validation/engine.mjs";

const root = "d:/Turing/Projects/workspace/task-output";
for (const taskId of [15790, 15792]) {
  const taskDir = path.join(root, String(taskId));
  const logFile = path.join(taskDir, "_logs", `tmp_validate_${taskId}_latest.log`);
  const result = await runNativeValidation(taskId, taskDir, logFile);
  console.log(JSON.stringify({ taskId, validatorsPassed: result.validatorsPassed, validatorsFailed: result.validatorsFailed, itemsFailed: result.itemsFailed }, null, 2));
}
