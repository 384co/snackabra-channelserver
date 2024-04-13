import { ChildProcessWithoutNullStreams, spawn } from 'child_process'
import stripAnsi from 'strip-ansi'

class WranglerMonitor {
	private process: ChildProcessWithoutNullStreams | null = null

	public start(): void {
		this.stop() // Ensure any existing process is stopped
		console.log(`Starting wrangler...`)
		this.process = spawn('yarn', ['start'], {
			env: {
				NODE_ENV: 'development',
				...process.env,
			},
		})

		this.process.stdout.on('data', (data: Buffer) => {
			this.handleOutput(stripAnsi(data.toString().replace('\r', '').trim()))
		})

		this.process.stderr.on('data', (data: Buffer) => {
			this.handleOutput(stripAnsi(data.toString().replace('\r', '').trim()), true)
		})
	}

	private handleOutput(output: string, err = false): void {
		if (!output) return
		if (output.includes('Segmentation fault')) {
			console.error('Segfault detected. Restarting Wrangler...')
			this.restart()
		} else if (output.includes('503 Service Unavailable')) {
			console.error('503 detected. Restarting Wrangler...')
			this.restart()
		}else if (!err) {
			console.log(output.replace('[mf:inf]', ''))
		}
	}

	private restart(): void {
		console.log('Restarting wrangler...')
		this.stop()
		setTimeout(() => this.start(), 100) // Restart after a short delay
	}

	private stop(): void {
		if (this.process) {
			this.process.kill()
			this.process = null
		}
	}
}

new WranglerMonitor().start()