/// <reference path="/root/.pnpm/global/5/node_modules/@types/node/index.d.ts" />

process.stdin.addListener("data", async (data) => {
	// console.log("Node received data");
	try {
		const message = JSON.parse(data.toString());
		console.log(`[${message.id}] recived: ${message}`);
		process.stdout.write(
			JSON.stringify({ id: message.id, data: Math.random() + message.data }) +
				"\n",
		);
	} catch (error) {
		// console.error(error);
		process.stderr.write(JSON.stringify({ data: error.message }) + "\n");
	}
});

console.log("Node process started");
process.stdin.resume();
