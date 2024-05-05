import { open } from "node:fs/promises"
import { createInterface } from "node:readline"
import { isMainThread, Worker, workerData, parentPort, threadId } from "node:worker_threads"

const NUM_THREADS = 31

if (isMainThread) {
	// MAIN
	function runWorker({ globalMeasurements, fileName, start, end }) {
		return new Promise((resolve, reject) => {
			const worker = new Worker(import.meta.filename, { workerData: { fileName, start, end } })

			function onMessage(localMeasurements) {
				Object.entries(localMeasurements).forEach(([station, { Min, Max, Sum, Count }]) => {
					const globalMeasurement = globalMeasurements[station]
					if (!globalMeasurement) {
						globalMeasurements[station] = {
							Min,
							Max,
							Sum,
							Count,
						}
					} else {
						globalMeasurement.Count += Count
						globalMeasurement.Sum += Sum
						globalMeasurement.Min = Math.min(Min, globalMeasurement.Min)
						globalMeasurement.Max = Math.max(Max, globalMeasurement.Max)
					}
				})
			}

			function onExit(code) {
				if (code !== 0) {
					reject(false)
				} else {
					resolve(true)
				}
			}

			worker.on("message", onMessage)
			worker.on("exit", onExit)
		})
	}

	const globalMeasurements = {}
	const fileName = "../1brc.data/measurements-10000000.txt"
	const fileHandle = await open(fileName, "r")
	const fileStat = await fileHandle.stat()
	const fileSizeInBytes = fileStat.size
	const chunkSize = Math.round(fileSizeInBytes / NUM_THREADS)
	const workers = []

	let lastPos = 0
	for (let i = 0; i < NUM_THREADS; i++) {
		const position = chunkSize * (i + 1)
		const buffer = new Uint8Array(4 * 1024)
		const { bytesRead } = await fileHandle.read(buffer, 0, buffer.byteLength, position)
		if (bytesRead === 0) {
			continue
		}

		const lastNewLineIndex = buffer.lastIndexOf(10)
		if (lastNewLineIndex === -1) {
			throw new Error("There is no newline in the buffer")
		}

		const absoluteLastNewLineIndex = position + lastNewLineIndex
		workers.push(runWorker({ globalMeasurements, fileName, start: lastPos, end: absoluteLastNewLineIndex - 1 }))
		lastPos = absoluteLastNewLineIndex + 1
	}

	// process remaining
	if (lastPos < fileSizeInBytes) {
		workers.push(runWorker({ globalMeasurements, fileName, start: lastPos, end: fileSizeInBytes }))
	}

	await fileHandle.close()
	await Promise.all(workers)

	const sortedStations = Object.keys(globalMeasurements).sort()

	process.stdout.write("{")
	sortedStations.forEach((station, stationIdx) => {
		const { Min, Max, Sum, Count } = globalMeasurements[station]
		const Mean = (Sum / Count).toFixed(1)

		process.stdout.write(`${station}=${Min}/${Mean}/${Max}`)
		if (stationIdx !== sortedStations.length - 1) {
			process.stdout.write(", ")
		}
	})
	process.stdout.write("}")
} else {
	// WORKER
	const localMeasurements = {}
	const { fileName, start, end } = workerData
	const fileHandle = await open(fileName, "r")

	const readLineInterface = createInterface({
		input: fileHandle.createReadStream({ start, end }),
		crlfDelay: Infinity
	})

	for await (const line of readLineInterface) {
		const [stationStr, tempStr] = line.split(";")
		const tempFloat = parseFloat(tempStr);
		const measurement = localMeasurements[stationStr]
		if (!measurement) {
			localMeasurements[stationStr] = {
				Min: tempFloat,
				Max: tempFloat,
				Sum: tempFloat,
				Count: 1,
			}
		} else {
			measurement.Count++
			measurement.Sum += tempFloat
			measurement.Min = Math.min(tempFloat, measurement.Min)
			measurement.Max = Math.max(tempFloat, measurement.Max)
		}
	}

	parentPort.postMessage(localMeasurements)
}
